/*-------------------------------------------------------------------------
 *
 * wait_event_timing.c
 *	  Per-backend wait event timing and histogram accumulation.
 *
 * This module provides Oracle-style wait event instrumentation: every
 * call to pgstat_report_wait_start()/pgstat_report_wait_end() records
 * the wait duration using clock_gettime() and accumulates per-event
 * statistics (count, total nanoseconds, max, histogram) in shared memory.
 *
 * Overhead: two VDSO clock_gettime() calls per wait event transition
 * (~40-100 ns total), plus a few memory writes to per-backend arrays.
 * No locking is needed since each backend writes only to its own slot.
 *
 * Controlled by the wait_event_capture GUC (off | stats | trace,
 * default off).  The 'stats' level activates the aggregated per-event
 * counters; 'trace' additionally enables a per-session DSA-backed ring
 * buffer of individual events for 10046-style analysis.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/wait_event_timing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/guc.h"
#include "utils/wait_event_timing.h"

/*
 * GUC variable -- always defined so the GUC system works even when
 * compiled without --enable-wait-event-timing.  In stub builds the
 * check_hook below rejects any value other than OFF.
 */
int			wait_event_capture = WAIT_EVENT_CAPTURE_OFF;

/*
 * Enum value table consumed by guc.c.  Order matches the
 * WaitEventCaptureLevel enum and the documented "off < stats < trace"
 * ordering.
 */
const struct config_enum_entry wait_event_capture_options[] = {
	{"off", WAIT_EVENT_CAPTURE_OFF, false},
	{"stats", WAIT_EVENT_CAPTURE_STATS, false},
	{"trace", WAIT_EVENT_CAPTURE_TRACE, false},
	{NULL, 0, false}
};

StaticAssertDecl(lengthof(wait_event_capture_options) == (WAIT_EVENT_CAPTURE_TRACE + 2),
				 "wait_event_capture_options length mismatch");

#ifndef USE_WAIT_EVENT_TIMING

/*
 * Stub SQL functions when compiled without --enable-wait-event-timing.
 * These are referenced by pg_proc.dat and must exist as symbols.
 */
#include "fmgr.h"
#include "funcapi.h"
#include "utils/guc_hooks.h"

Datum		pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS);
Datum		pg_get_backend_wait_event_trace(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS);
Datum		pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS);

Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_get_backend_wait_event_trace(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("wait event capture is not supported by this build"),
			 errhint("Compile PostgreSQL with --enable-wait-event-timing.")));
	PG_RETURN_VOID();
}

/*
 * Extern variables referenced by backend_status.c unconditionally.
 * In timing builds these are defined after the #else.
 */
/*
 * GUC check hook for the stub build.  Any value other than 'off' is
 * meaningless without --enable-wait-event-timing, so we reject it
 * (or downgrade to 'off' silently when the value comes from the
 * config file at startup, matching the old per-GUC behavior).
 */
bool
check_wait_event_capture(int *newval, void **extra, GucSource source)
{
	if (*newval != WAIT_EVENT_CAPTURE_OFF)
	{
		if (source < PGC_S_INTERACTIVE)
		{
			ereport(WARNING,
					(errmsg("wait_event_capture is not supported by this build, "
							"forcing to \"off\""),
					 errhint("Compile PostgreSQL with "
							 "--enable-wait-event-timing.")));
			*newval = WAIT_EVENT_CAPTURE_OFF;
			return true;
		}
		GUC_check_errdetail("This build does not support wait event capture.");
		GUC_check_errhint("Compile PostgreSQL with --enable-wait-event-timing.");
		return false;
	}
	return true;
}

/* Stub GUC assign hook -- nothing to do without compile-time support. */
void
assign_wait_event_capture(int newval, void *extra)
{
}

/* Stub shmem functions called from ipci.c */
Size
WaitEventTimingShmemSize(void)
{
	return 0;
}

void
WaitEventTimingShmemInit(void)
{
}

Size
WaitEventTraceControlShmemSize(void)
{
	return 0;
}

void
WaitEventTraceControlShmemInit(void)
{
}

void
pgstat_set_wait_event_timing_storage(int procNumber)
{
}

void
pgstat_reset_wait_event_timing_storage(void)
{
}

#else							/* USE_WAIT_EVENT_TIMING */

#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "catalog/pg_type_d.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

#define NUM_WAIT_EVENT_TIMING_SLOTS  (MaxBackends + NUM_AUXILIARY_PROCS)

#define HAS_PGSTAT_PERMISSIONS(role) \
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

/* Pointer to this backend's timing state */
WaitEventTimingState *my_wait_event_timing = NULL;

/* Pointer to this backend's trace ring buffer */
WaitEventTraceState *my_wait_event_trace = NULL;

/*
 * Backend-local copy of the last reset generation we acted on.  Compared
 * against the shared pg_atomic_uint32 reset_generation in this backend's
 * WaitEventTimingState slot at every wait_end.  When the shared value
 * differs, the owning backend performs the reset of its own counters on
 * behalf of whoever called pg_stat_reset_wait_event_timing(target).
 *
 * This makes cross-backend reset a lock-free request-response: the caller
 * bumps the atomic (and wakes the target's latch so idle backends notice);
 * the owning backend clears its counters at a safe point.  Because only the
 * owning backend ever writes its slot, there is no race between writers and
 * resetters -- the reset happens inline inside the single-writer hot path.
 */
static uint32 my_last_reset_generation = 0;

/*
 * DSA-based shared timing array control.
 *
 * The per-backend WaitEventTimingState array is allocated lazily in DSA
 * on the first SET wait_event_capture = stats|trace in the cluster.
 * This avoids ~11-113 MB of eager shmem allocation at postmaster start
 * when the feature is compiled in but turned off at runtime (the common
 * case).  See wait_event_timing_attach_array().
 *
 * The control struct itself lives in the small fixed shmem region; it
 * holds a DSA handle and a dsa_pointer to the allocated array.
 */
typedef struct WaitEventTimingControl
{
	LWLock		lock;			/* protects first-time DSA create + array alloc */
	dsa_handle	timing_dsa_handle;	/* DSA_HANDLE_INVALID until first enable */
	dsa_pointer timing_array;	/* InvalidDsaPointer until first enable */
} WaitEventTimingControl;

static WaitEventTimingControl *WaitEventTimingCtl = NULL;
static dsa_area *timing_dsa = NULL;

/*
 * Backend-local cached pointer to the shared array, set on first
 * lazy-attach.  Readers of other backends' slots (pg_stat_*) attach
 * on demand and use this cache for the rest of the SRF call.  Writers
 * access their own slot exclusively via my_wait_event_timing.
 */
static WaitEventTimingState *WaitEventTimingArray = NULL;

/* DSA-based trace ring buffer control */
static WaitEventTraceControl *WaitEventTraceCtl = NULL;
static dsa_area *trace_dsa = NULL;
int			my_trace_proc_number = -1;

/* Forward declarations for lazy-attach helpers */
static void wait_event_timing_ensure_dsa(void);
static bool wait_event_timing_attach_array(bool allocate_if_missing);

/*
 * Mapping arrays for the flat events[] array, generated from
 * wait_event_names.txt by generate-wait_event_types.pl.
 * Defines: WAIT_EVENT_TIMING_RAW_CLASSES, WAIT_EVENT_TIMING_DENSE_CLASSES,
 *          WAIT_EVENT_TIMING_NUM_EVENTS, and the four mapping arrays.
 */
#include "utils/wait_event_timing_data.c"

/*
 * Convert wait_event_info to a flat index for the events[] array.
 * For bounded classes, eventId equals the array index within the class
 * (the enum values start at PG_WAIT_<CLASS> and increment by one).
 */
static int
wait_event_timing_index(uint32 wait_event_info)
{
	int			classId = (wait_event_info >> 24) & 0xFF;
	int			eventId = wait_event_info & 0xFFFF;	/* array index for bounded classes */
	int			dense;

	if (classId == (PG_WAIT_LWLOCK >> 24))
		return WAIT_EVENT_TIMING_IDX_LWLOCK;

	if (unlikely(classId >= WAIT_EVENT_TIMING_RAW_CLASSES))
		return -1;

	dense = wait_event_class_dense[classId];
	if (unlikely(dense < 0))
		return -1;

	if (unlikely(eventId >= wait_event_class_nevents[dense]))
		return -1;

	return wait_event_class_offset[dense] + eventId;
}

/*
 * Look up (or insert) timing entry for an LWLock tranche ID.
 */
static WaitEventTimingEntry *
lwlock_timing_lookup(LWLockTimingHash *ht, uint16 tranche_id)
{
	uint32		hash = (uint32) tranche_id * 2654435761U;
	int			slot = hash & (LWLOCK_TIMING_HASH_SIZE - 1);
	int			i;

	for (i = 0; i < LWLOCK_TIMING_HASH_SIZE; i++)
	{
		LWLockTimingHashEntry *e = &ht->entries[slot];

		if (e->tranche_id == tranche_id)
			return &ht->lwlock_events[e->dense_idx];

		if (e->tranche_id == 0)
		{
			if (ht->num_used >= LWLOCK_TIMING_MAX_ENTRIES)
				return NULL;

			e->tranche_id = tranche_id;
			e->dense_idx = ht->num_used++;
			return &ht->lwlock_events[e->dense_idx];
		}

		slot = (slot + 1) & (LWLOCK_TIMING_HASH_SIZE - 1);
	}

	return NULL;
}

/*
 * Compute histogram bucket index for a duration in nanoseconds.
 */
static int
wait_event_timing_bucket(int64 duration_ns)
{
	int64		duration_us = duration_ns / 1000;
	int			bucket;

	if (duration_us <= 0)
		return 0;

	bucket = pg_leftmost_one_pos64((uint64) duration_us) + 1;

	if (bucket >= WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS)
		bucket = WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS - 1;

	return bucket;
}

/*
 * Write a trace ring marker record.  Shared helper for all marker types.
 */
static void
wait_event_trace_write_marker(uint8 record_type, int64 query_id)
{
	uint64	pos;
	WaitEventTraceRecord *rec;
	uint32	seq;
	instr_time now;

	/*
	 * Single capture-level gate: markers only land in the ring when
	 * wait_event_capture is at TRACE.  This guarantees consistency with
	 * the wait-event hot path (also gated on the same level) -- there is
	 * no configuration in which one half of the trace fires and the
	 * other doesn't.  query_id == 0 means "no query ID available"
	 * (utility command or compute_query_id = off), which we skip.
	 */
	if (likely(wait_event_capture != WAIT_EVENT_CAPTURE_TRACE || query_id == 0))
		return;

	/*
	 * Lazy attach on first use.  Allocation lives here (not in the
	 * assign hook) because dsa_allocate_extended() can ereport(ERROR)
	 * on OOM, which is forbidden in assign-hook context but legitimate
	 * here.  Idempotent: wait_event_trace_attach() short-circuits on
	 * subsequent calls.
	 */
	if (my_wait_event_trace == NULL)
	{
		if (my_trace_proc_number < 0)
			return;
		wait_event_trace_attach(my_trace_proc_number);
		if (my_wait_event_trace == NULL)
			return;			/* attach path unable to allocate */
	}

	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
	rec = &my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
	seq = (uint32)(pos * 2 + 1);

	rec->seq = seq;
	pg_write_barrier();

	INSTR_TIME_SET_CURRENT(now);
	rec->record_type = record_type;
	rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
	rec->data.query.query_id = query_id;
	rec->data.query.pad2 = 0;

	pg_write_barrier();
	rec->seq = seq + 1;
}

void
wait_event_trace_query_start(int64 query_id)
{
	wait_event_trace_write_marker(TRACE_QUERY_START, query_id);
}

void
wait_event_trace_query_end(int64 query_id)
{
	wait_event_trace_write_marker(TRACE_QUERY_END, query_id);
}

void
wait_event_trace_exec_start(int64 query_id)
{
	wait_event_trace_write_marker(TRACE_EXEC_START, query_id);
}

void
wait_event_trace_exec_end(int64 query_id)
{
	wait_event_trace_write_marker(TRACE_EXEC_END, query_id);
}

/*
 * Report the shared memory space needed.
 *
 * Only the small control struct is in fixed shmem.  The per-backend
 * WaitEventTimingState array (~11-113 MB depending on max_connections)
 * is allocated lazily in DSA on first enable by any backend.
 */
Size
WaitEventTimingShmemSize(void)
{
	return sizeof(WaitEventTimingControl);
}

/*
 * Initialize shared memory for wait event timing.
 *
 * Only a small control struct is created here.  The actual per-backend
 * timing array is allocated via DSA the first time a backend sets
 * wait_event_capture to a non-OFF value (see wait_event_timing_attach_array).
 */
void
WaitEventTimingShmemInit(void)
{
	bool		found;

	WaitEventTimingCtl = (WaitEventTimingControl *)
		ShmemInitStruct("WaitEventTimingControl",
						sizeof(WaitEventTimingControl), &found);

	if (!found)
	{
		LWLockInitialize(&WaitEventTimingCtl->lock,
						 LWTRANCHE_WAIT_EVENT_TIMING_DSA);
		WaitEventTimingCtl->timing_dsa_handle = DSA_HANDLE_INVALID;
		WaitEventTimingCtl->timing_array = InvalidDsaPointer;
	}

	WaitEventTimingArray = NULL;
}

/*
 * Ensure the backend is attached to the timing DSA.
 *
 * The DSA is created by whichever backend first hits this function with
 * an empty control struct; subsequent callers just attach to the
 * existing handle.  The backend-local dsa_area pointer is cached in
 * timing_dsa for the backend's lifetime.
 */
static void
wait_event_timing_ensure_dsa(void)
{
	MemoryContext oldcontext;

	if (timing_dsa != NULL)
		return;

	if (WaitEventTimingCtl == NULL)
		return;					/* pre-ShmemInit; nothing to attach to */

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(&WaitEventTimingCtl->lock, LW_EXCLUSIVE);

	if (WaitEventTimingCtl->timing_dsa_handle == DSA_HANDLE_INVALID)
	{
		timing_dsa = dsa_create(LWTRANCHE_WAIT_EVENT_TIMING_DSA);
		dsa_pin(timing_dsa);
		dsa_pin_mapping(timing_dsa);
		WaitEventTimingCtl->timing_dsa_handle = dsa_get_handle(timing_dsa);
	}
	else
	{
		timing_dsa = dsa_attach(WaitEventTimingCtl->timing_dsa_handle);
		dsa_pin_mapping(timing_dsa);
	}

	LWLockRelease(&WaitEventTimingCtl->lock);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Attach this backend to the shared WaitEventTimingArray, allocating
 * it in DSA on first use if allocate_if_missing is true.
 *
 * Returns true if the array is now available (WaitEventTimingArray is
 * non-NULL on return); false otherwise.  Readers pass allocate_if_missing
 * = false to avoid allocating a big array just because somebody ran
 * SELECT against an empty pg_stat view.  Writers (hot path) pass true
 * so that the first wait event under wait_event_capture != off creates
 * the storage.
 *
 * Re-entrancy guard: dsa_create() / dsa_allocate_extended() below can
 * emit LWLock wait events internally, which reach the hot path and
 * re-enter this function.  Without the guard we would deadlock on
 * WaitEventTimingCtl->lock.
 */
static bool
wait_event_timing_attach_array(bool allocate_if_missing)
{
	static bool in_attach = false;
	bool		attached = false;

	if (WaitEventTimingArray != NULL)
		return true;

	if (WaitEventTimingCtl == NULL)
		return false;

	if (in_attach)
		return false;

	in_attach = true;
	PG_TRY();
	{
		wait_event_timing_ensure_dsa();

		if (WaitEventTimingCtl->timing_array == InvalidDsaPointer)
		{
			if (!allocate_if_missing)
			{
				attached = false;
			}
			else
			{
				Size		size;

				size = mul_size(NUM_WAIT_EVENT_TIMING_SLOTS,
								sizeof(WaitEventTimingState));

				LWLockAcquire(&WaitEventTimingCtl->lock, LW_EXCLUSIVE);

				if (WaitEventTimingCtl->timing_array == InvalidDsaPointer)
				{
					dsa_pointer p;
					WaitEventTimingState *array;

					p = dsa_allocate_extended(timing_dsa, size,
											  DSA_ALLOC_ZERO);
					array = (WaitEventTimingState *)
						dsa_get_address(timing_dsa, p);

					for (int i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
						pg_atomic_init_u32(&array[i].reset_generation, 0);

					WaitEventTimingCtl->timing_array = p;
				}

				LWLockRelease(&WaitEventTimingCtl->lock);

				attached = true;
			}
		}
		else
		{
			attached = true;
		}

		if (attached)
			WaitEventTimingArray = (WaitEventTimingState *)
				dsa_get_address(timing_dsa,
								WaitEventTimingCtl->timing_array);
	}
	PG_FINALLY();
	{
		in_attach = false;
	}
	PG_END_TRY();

	return WaitEventTimingArray != NULL;
}

/*
 * Point my_wait_event_timing at this backend's slot within the shared
 * timing array, allocating the array in DSA on first call.
 *
 * Called from the hot path (pgstat_report_wait_start / _end) out of line
 * when wait_event_capture is non-OFF and my_wait_event_timing is still
 * NULL.  Also called explicitly from the reset SQL function to provide
 * synchronous "reset own backend" semantics immediately after SET
 * wait_event_capture = stats.
 */
void
pgstat_wait_event_timing_lazy_attach(void)
{
	int			procNumber;

	if (my_wait_event_timing != NULL)
		return;

	if (MyProc == NULL)
		return;

	procNumber = GetNumberFromPGProc(MyProc);
	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	if (!wait_event_timing_attach_array(true))
		return;

	my_wait_event_timing = &WaitEventTimingArray[procNumber];

	/*
	 * Clear this backend's slot the first time it is used after backend
	 * start.  The DSA-allocated region is zeroed on creation, but a later
	 * backend may inherit a slot previously occupied by an exited
	 * backend; explicit zero here keeps stats accurate across slot reuse.
	 * Matches the old per-backend init performed by
	 * pgstat_set_wait_event_timing_storage() in the eager-shmem design.
	 */
	memset(my_wait_event_timing->events, 0,
		   sizeof(my_wait_event_timing->events));
	memset(&my_wait_event_timing->lwlock_hash, 0,
		   sizeof(LWLockTimingHash));
	my_wait_event_timing->reset_count = 0;
	my_wait_event_timing->lwlock_overflow_count = 0;
	my_wait_event_timing->flat_overflow_count = 0;
	my_wait_event_timing->current_event = 0;
	INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);

	my_last_reset_generation =
		pg_atomic_read_u32(&my_wait_event_timing->reset_generation);
}

/*
 * Report the shared memory space needed for trace ring buffer control.
 * Only a small control struct is in fixed shmem; the actual ring buffers
 * are allocated lazily via DSA.
 */
Size
WaitEventTraceControlShmemSize(void)
{
	return add_size(offsetof(WaitEventTraceControl, trace_ptrs),
					mul_size(NUM_WAIT_EVENT_TIMING_SLOTS, sizeof(dsa_pointer)));
}

/*
 * Initialize shared memory for trace ring buffer control.
 */
void
WaitEventTraceControlShmemInit(void)
{
	bool		found;
	Size		size;

	size = WaitEventTraceControlShmemSize();

	WaitEventTraceCtl = (WaitEventTraceControl *)
		ShmemInitStruct("WaitEventTraceControl", size, &found);

	if (!found)
	{
		int		i;

		WaitEventTraceCtl->trace_dsa_handle = DSA_HANDLE_INVALID;
		LWLockInitialize(&WaitEventTraceCtl->lock,
						 LWTRANCHE_WAIT_EVENT_TRACE_DSA);
		for (i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
			WaitEventTraceCtl->trace_ptrs[i] = InvalidDsaPointer;
	}
}

/*
 * Ensure the shared DSA for trace ring buffers exists and is attached.
 * Creates it on first call (any backend), attaches on subsequent calls.
 * Must be called from a backend context (not postmaster).
 */
void
wait_event_trace_ensure_dsa(void)
{
	MemoryContext oldcontext;

	if (trace_dsa != NULL)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);

	if (WaitEventTraceCtl->trace_dsa_handle == DSA_HANDLE_INVALID)
	{
		trace_dsa = dsa_create(LWTRANCHE_WAIT_EVENT_TRACE_DSA);
		dsa_pin(trace_dsa);
		dsa_pin_mapping(trace_dsa);
		WaitEventTraceCtl->trace_dsa_handle = dsa_get_handle(trace_dsa);
	}
	else
	{
		trace_dsa = dsa_attach(WaitEventTraceCtl->trace_dsa_handle);
		dsa_pin_mapping(trace_dsa);
	}

	LWLockRelease(&WaitEventTraceCtl->lock);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Free trace ring buffer for this backend.
 *
 * Must be called BEFORE dsm_backend_shutdown() detaches the DSA.
 * Registered as a before_shmem_exit callback.
 */
static void
wait_event_trace_before_shmem_exit(int code, Datum arg)
{
	int		procNumber = DatumGetInt32(arg);

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	if (DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[procNumber]) &&
		trace_dsa != NULL)
	{
		LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);
		dsa_free(trace_dsa, WaitEventTraceCtl->trace_ptrs[procNumber]);
		WaitEventTraceCtl->trace_ptrs[procNumber] = InvalidDsaPointer;
		LWLockRelease(&WaitEventTraceCtl->lock);
	}

	my_wait_event_trace = NULL;
}

/*
 * Allocate a trace ring buffer for this backend via DSA.
 * Called when wait_event_capture is set to 'trace'.
 */
void
wait_event_trace_attach(int procNumber)
{
	/*
	 * Re-entrancy guard.  dsa_create() / dsa_allocate_extended() below
	 * can emit wait events internally (LWLock sleeps during shmem
	 * allocation, DSM OS calls, ...), and those wait events will reach
	 * pgstat_report_wait_end_timing() and wait_event_trace_write_marker(),
	 * both of which perform lazy attach when my_wait_event_trace is
	 * still NULL.  Without this guard the recursive call tries to
	 * re-acquire WaitEventTraceCtl->lock, producing a self-deadlock.
	 */
	static bool in_attach = false;
	dsa_pointer p;
	WaitEventTraceState *ts;

	if (in_attach)
		return;

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	in_attach = true;
	PG_TRY();
	{
		/* Already have a ring buffer? */
		if (DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[procNumber]))
		{
			wait_event_trace_ensure_dsa();
			my_wait_event_trace = dsa_get_address(trace_dsa,
												  WaitEventTraceCtl->trace_ptrs[procNumber]);
			my_trace_proc_number = procNumber;
		}
		else
		{
			wait_event_trace_ensure_dsa();

			p = dsa_allocate_extended(trace_dsa, sizeof(WaitEventTraceState),
									  DSA_ALLOC_ZERO);
			ts = dsa_get_address(trace_dsa, p);
			pg_atomic_init_u64(&ts->write_pos, 0);

			LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);
			WaitEventTraceCtl->trace_ptrs[procNumber] = p;
			LWLockRelease(&WaitEventTraceCtl->lock);

			my_wait_event_trace = ts;
			my_trace_proc_number = procNumber;

			/*
			 * Register cleanup to run BEFORE dsm_backend_shutdown()
			 * detaches the DSA.  The before_shmem_exit callbacks run in
			 * LIFO order before DSM detach, so dsa_free() is safe at
			 * that point.
			 *
			 * This branch executes at most once per backend lifetime:
			 * subsequent SET wait_event_capture = trace takes the
			 * reattach fast path above because trace_ptrs[procNumber]
			 * remains valid until exit.
			 */
			before_shmem_exit(wait_event_trace_before_shmem_exit,
							  Int32GetDatum(procNumber));
		}
	}
	PG_FINALLY();
	{
		in_attach = false;
	}
	PG_END_TRY();
}

/*
 * Free trace ring buffer for this backend on exit.
 */
void
wait_event_trace_detach(int procNumber)
{
	/*
	 * Only clear local pointers here.  The actual DSA free happens in
	 * wait_event_trace_before_shmem_exit(), which runs before
	 * dsm_backend_shutdown() detaches the DSA segments.
	 */
	my_wait_event_trace = NULL;
	my_trace_proc_number = -1;
}

/*
 * GUC check hook for wait_event_capture (timing build).
 *
 * All three enum values are accepted at this level; the assign hook
 * handles side effects (attaching the trace ring on TRACE, warning
 * about track_activities, etc.).
 */
bool
check_wait_event_capture(int *newval, void **extra, GucSource source)
{
	return true;
}

/*
 * GUC assign hook for wait_event_capture.
 *
 * Two responsibilities, both correctness-critical:
 *
 * 1) Drop any in-flight wait state.  After the capture level changes,
 *    the existing wait_start / current_event in our per-backend slot can
 *    no longer be trusted.  Consider this sequence:
 *
 *       capture = STATS, wait on E1 starts -> wait_start=T0, current_event=E1
 *       capture flips to OFF mid-wait
 *       wait_end inline skips (guard fails) -> state still T0/E1
 *       new wait on E2 starts under OFF     -> inline skips, state still T0/E1
 *       capture flips back to STATS
 *       wait_end for E2 -> guard passes, credits (now - T0) to E1
 *
 *    Zeroing both fields on every assignment forfeits at most one
 *    in-flight sample per GUC change (negligible) but eliminates all
 *    such miscredits.
 *
 * 2) Warn (but never error) about secondary preconditions for TRACE
 *    level.  GUC assign hooks MUST NOT ereport(ERROR) -- see
 *    src/backend/utils/misc/README -- because they can run during
 *    transaction rollback when lookups are unsafe.  In particular, the
 *    trace ring's DSA allocation is NOT performed here (it can raise on
 *    OOM).  Instead, the ring is attached lazily on the first write
 *    from wait_event_trace_write_marker() and
 *    pgstat_report_wait_end_timing(), where ereport(ERROR) has
 *    well-defined semantics.  Stepping down from TRACE to STATS/OFF
 *    intentionally does not detach the ring either: keeping the segment
 *    around lets a single session toggle tracing on and off cheaply
 *    during an investigation; the ring is released on backend exit via
 *    the before_shmem_exit callback registered in
 *    wait_event_trace_attach().
 */
void
assign_wait_event_capture(int newval, void *extra)
{
	if (my_wait_event_timing != NULL)
	{
		INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
		my_wait_event_timing->current_event = 0;
	}

	if (newval == WAIT_EVENT_CAPTURE_TRACE && !pgstat_track_activities)
		ereport(WARNING,
				(errmsg("wait_event_capture = trace query attribution "
						"requires track_activities to be enabled")));
}

/*
 * Point my_wait_event_timing at this backend's slot.
 * Called from InitProcess() after the backend has a valid procNumber.
 *
 * procNumber is the PGPROC array index (from GetNumberFromPGProc).
 * Covers both regular backends (procNumber < MaxBackends) and auxiliary
 * processes (bgwriter, checkpointer, walwriter, etc.).
 *
 * On EXEC_BACKEND builds (Windows), SubPostmasterMain() calls
 * CreateSharedMemoryAndSemaphores() before InitProcess(), so
 * WaitEventTimingArray is always initialized at this point.
 */
void
pgstat_set_wait_event_timing_storage(int procNumber)
{
	/*
	 * Do NOT attach to the timing array here: the array is allocated in
	 * DSA on first enable of wait_event_capture (see
	 * pgstat_wait_event_timing_lazy_attach).  A backend that never enables
	 * capture pays zero shmem cost.
	 *
	 * Trace ring buffer is allocated lazily via DSA when
	 * wait_event_capture is set to 'trace'.  Save procNumber for later
	 * use by trace_attach/detach.
	 */
	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
	{
		my_wait_event_timing = NULL;
		my_trace_proc_number = -1;
		my_wait_event_trace = NULL;
		return;
	}

	my_wait_event_timing = NULL;
	my_trace_proc_number = procNumber;
	my_wait_event_trace = NULL;
}

/*
 * Detach from timing state on backend exit.
 *
 * This function is invoked from ProcKill() as an on_shmem_exit callback,
 * which runs AFTER dsm_backend_shutdown() has detached DSA mappings.
 * Writing to my_wait_event_timing at this point would touch DSA-backed
 * memory that is no longer mapped and would segfault.
 *
 * We therefore only clear the backend-local pointers here.  Zeroing of
 * the shared slot itself happens in two safe places:
 *   - the next time a backend attaches to the slot (lazy_attach memsets),
 *   - the SRF readers filter dead backends via pgstat_get_beentry_by_proc_number,
 * so stale data in the slot never becomes user-visible.
 */
void
pgstat_reset_wait_event_timing_storage(void)
{
	/* Trace ring buffer: cleanup via before_shmem_exit callback (Fix #1) */
	if (my_trace_proc_number >= 0)
		wait_event_trace_detach(my_trace_proc_number);

	my_wait_event_timing = NULL;
	my_wait_event_trace = NULL;
	my_trace_proc_number = -1;
}

/*
 * Out-of-line body for pgstat_report_wait_end() timing path.
 * Called when wait_event_capture is at STATS or higher and
 * my_wait_event_timing is set.  Computes wait duration, accumulates
 * per-event stats, and (at TRACE level) writes the event into the
 * per-session trace ring buffer.
 */
void
pgstat_report_wait_end_timing(void)
{
	uint32		event = my_wait_event_timing->current_event;
	uint32		cur_reset_gen;

	/*
	 * Fast check for a pending cross-backend reset request.  Single
	 * atomic load; almost always hits the fast path (branch well
	 * predicted).  When we detect that our shared reset_generation has
	 * advanced, clear our own counters on behalf of the requester, then
	 * continue with normal accumulation.  The in-flight wait fields
	 * (wait_start / current_event) are deliberately left untouched so we
	 * don't lose the measurement that's already running; the completing
	 * event will land in the freshly-zeroed counters, which is the
	 * desired behaviour.
	 */
	cur_reset_gen = pg_atomic_read_u32(&my_wait_event_timing->reset_generation);
	if (unlikely(cur_reset_gen != my_last_reset_generation))
	{
		memset(my_wait_event_timing->events, 0,
			   sizeof(my_wait_event_timing->events));
		memset(&my_wait_event_timing->lwlock_hash, 0,
			   sizeof(LWLockTimingHash));
		my_wait_event_timing->reset_count++;
		my_wait_event_timing->lwlock_overflow_count = 0;
		my_wait_event_timing->flat_overflow_count = 0;
		my_last_reset_generation = cur_reset_gen;
	}

	if (event != 0 && !INSTR_TIME_IS_ZERO(my_wait_event_timing->wait_start))
	{
		instr_time	now;
		int64		duration_ns;
		int			idx;

		INSTR_TIME_SET_CURRENT(now);
		duration_ns = INSTR_TIME_GET_NANOSEC(now) -
			INSTR_TIME_GET_NANOSEC(my_wait_event_timing->wait_start);

		if (unlikely(duration_ns < 0))
			duration_ns = 0;

		idx = wait_event_timing_index(event);

		/*
		 * No lock needed on the hot path: each WaitEventTimingState slot
		 * has a single writer (the owning backend), and the SRF reader
		 * pg_stat_get_wait_event_timing() is lock-free by design.  Cross-
		 * backend reset is handled by the reset_generation check at the
		 * top of this function: the requester bumps the atomic and the
		 * owning backend (us) clears the counters at the next wait_end.
		 *
		 * We defer emitting the overflow WARNING to after the critical
		 * bookkeeping is complete, so ereport() cannot recurse through
		 * a wait event while counters are in an intermediate state.
		 */
		{
			WaitEventTimingEntry *entry = NULL;
			bool		warn_lwlock_overflow = false;
			bool		warn_flat_overflow = false;

			if (idx == WAIT_EVENT_TIMING_IDX_LWLOCK)
				entry = lwlock_timing_lookup(
					&my_wait_event_timing->lwlock_hash,
					event & 0xFFFF);
			else if (likely(idx >= 0))
				entry = &my_wait_event_timing->events[idx];

			if (likely(entry != NULL))
			{
				entry->count++;
				entry->total_ns += duration_ns;
				if (duration_ns > entry->max_ns)
					entry->max_ns = duration_ns;
				entry->histogram[wait_event_timing_bucket(duration_ns)]++;
			}
			else if (idx == WAIT_EVENT_TIMING_IDX_LWLOCK)
			{
				if (my_wait_event_timing->lwlock_overflow_count++ == 0)
					warn_lwlock_overflow = true;
			}
			else if (idx == -1)
			{
				if (my_wait_event_timing->flat_overflow_count++ == 0)
					warn_flat_overflow = true;
			}

			/* Emit overflow warnings outside any critical section. */
			if (unlikely(warn_lwlock_overflow))
				ereport(WARNING,
						(errmsg("wait_event_timing: LWLock hash table full, "
								"timing data for some LWLock tranches will be lost"),
						 errhint("This backend uses more than %d distinct LWLock tranches.",
								 LWLOCK_TIMING_MAX_ENTRIES)));
			else if (unlikely(warn_flat_overflow))
				ereport(WARNING,
						(errmsg("wait_event_timing: event class overflow, "
								"some events will not be timed")));
		}

		/* 10046-style per-session trace ring buffer (DSA-backed) */
		if (unlikely(wait_event_capture == WAIT_EVENT_CAPTURE_TRACE))
		{
			/*
			 * Lazy attach on first use -- allocation happens here rather
			 * than in assign_wait_event_capture() to respect the GUC
			 * assign-hook "must not ereport" contract.  See the comment
			 * on assign_wait_event_capture() for rationale.
			 */
			if (my_wait_event_trace == NULL && my_trace_proc_number >= 0)
				wait_event_trace_attach(my_trace_proc_number);

			if (my_wait_event_trace != NULL)
			{
				uint64	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
				WaitEventTraceRecord *rec =
					&my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
				uint32	seq = (uint32)(pos * 2 + 1);

				rec->seq = seq;
				pg_write_barrier();

				rec->record_type = TRACE_WAIT_EVENT;
				rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
				rec->data.wait.event = event;
				rec->data.wait.pad2 = 0;
				rec->data.wait.duration_ns = duration_ns;

				pg_write_barrier();
				rec->seq = seq + 1;
			}
		}

		INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
	}
}

/*
 * SQL function: pg_stat_get_wait_event_timing(OUT ...)
 *
 * Returns one row per (backend_id, wait_event) with non-zero counts.
 *
 * Uses InitMaterializedSRF (materialize-all) for simplicity.  The result
 * set is bounded by (NUM_WAIT_EVENT_TIMING_SLOTS * WAIT_EVENT_TIMING_NUM_EVENTS)
 * rows, so deferred (value-per-call) mode is not needed.
 */
Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If no backend has ever enabled wait_event_capture since the last
	 * postmaster start, the shared timing array has not been allocated
	 * yet -- return zero rows rather than forcing an allocation just for
	 * a read.
	 */
	if (!wait_event_timing_attach_array(false))
		PG_RETURN_VOID();

	for (backend_idx = 0; backend_idx < NUM_WAIT_EVENT_TIMING_SLOTS; backend_idx++)
	{
		WaitEventTimingState *state = &WaitEventTimingArray[backend_idx];
		PgBackendStatus *beentry;
		int			i;

		/* Skip dead backend slots and check permissions */
		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			continue;
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			continue;

		/* Emit rows from the flat array (all classes except LWLock) */
		for (i = 0; i < WAIT_EVENT_TIMING_DENSE_CLASSES; i++)
		{
			int		base = wait_event_class_offset[i];
			int		nevents = wait_event_class_nevents[i];
			uint32	classId = wait_event_dense_to_classid[i];
			int		j;

			for (j = 0; j < nevents; j++)
			{
				WaitEventTimingEntry *entry = &state->events[base + j];
				Datum		values[10];
				bool		nulls[10];
				uint32		wait_event_info;
				const char *event_type;
				const char *event_name;
				int			bucket;

				if (entry->count == 0)
					continue;

				/* Reconstruct wait_event_info from class and event ID */
				wait_event_info = ((uint32) classId << 24) | j;

				event_type = pgstat_get_wait_event_type(wait_event_info);
				event_name = pgstat_get_wait_event(wait_event_info);

				if (event_type == NULL || event_name == NULL)
					continue;

				memset(nulls, 0, sizeof(nulls));

				values[0] = Int32GetDatum(beentry->st_procpid);
				values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
				values[2] = Int32GetDatum(backend_idx + 1);
				values[3] = CStringGetTextDatum(event_type);
				values[4] = CStringGetTextDatum(event_name);
				values[5] = Int64GetDatum(entry->count);
				values[6] = Float8GetDatum((double) entry->total_ns / 1000000.0);
				values[7] = Float8GetDatum(entry->count > 0
										   ? (double) entry->total_ns / entry->count / 1000.0
										   : 0.0);
				values[8] = Float8GetDatum((double) entry->max_ns / 1000.0);

				{
					Datum	elems[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];

					for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
						elems[bucket] = Int64GetDatum(entry->histogram[bucket]);

					values[9] = PointerGetDatum(
						construct_array_builtin(elems,
												WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS,
												INT8OID));
				}

				tuplestore_putvalues(rsinfo->setResult,
									rsinfo->setDesc,
									values, nulls);
			}
		}

		/* Emit rows from the LWLock hash table */
		for (i = 0; i < LWLOCK_TIMING_HASH_SIZE; i++)
		{
			LWLockTimingHashEntry *he = &state->lwlock_hash.entries[i];
			WaitEventTimingEntry *entry;
			Datum		values[10];
			bool		nulls[10];
			uint32		wait_event_info;
			const char *event_type;
			const char *event_name;
			int			bucket;

			if (he->tranche_id == 0)
				continue;

			entry = &state->lwlock_hash.lwlock_events[he->dense_idx];
			if (entry->count == 0)
				continue;

			wait_event_info = PG_WAIT_LWLOCK | he->tranche_id;

			event_type = pgstat_get_wait_event_type(wait_event_info);
			event_name = pgstat_get_wait_event(wait_event_info);

			if (event_type == NULL || event_name == NULL)
				continue;

			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(beentry->st_procpid);
			values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
			values[2] = Int32GetDatum(backend_idx + 1);
			values[3] = CStringGetTextDatum(event_type);
			values[4] = CStringGetTextDatum(event_name);
			values[5] = Int64GetDatum(entry->count);
			values[6] = Float8GetDatum((double) entry->total_ns / 1000000.0);
			values[7] = Float8GetDatum(entry->count > 0
									   ? (double) entry->total_ns / entry->count / 1000.0
									   : 0.0);
			values[8] = Float8GetDatum((double) entry->max_ns / 1000.0);

			{
				Datum	elems[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];

				for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
					elems[bucket] = Int64GetDatum(entry->histogram[bucket]);

				values[9] = PointerGetDatum(
					construct_array_builtin(elems,
											WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS,
											INT8OID));
			}

			tuplestore_putvalues(rsinfo->setResult,
								rsinfo->setDesc,
								values, nulls);
		}
	}

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_get_backend_wait_event_trace()
 *
 * Returns trace records from the current backend's own ring buffer.
 * Cross-backend ring reading is intentionally not supported: the ring
 * lives in per-backend DSA and reading another session's segment would
 * require attaching/detaching under the trace control lock, which is
 * the responsibility of external consumers (extensions, background
 * workers) that can manage their own synchronization via
 * WaitEventTraceCtl->lock.  The name mirrors
 * pg_get_backend_memory_contexts() to make the session-local scope
 * explicit at the API level.
 *
 * Uses InitMaterializedSRF (materialize-all).  The ring holds up to
 * WAIT_EVENT_TRACE_RING_SIZE (131072) records; full materialization is
 * acceptable for own-session diagnostics.
 */
Datum
pg_get_backend_wait_event_trace(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	WaitEventTraceState *ts;
	uint64		write_pos;
	uint64		read_start;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	if (my_wait_event_trace == NULL)
		PG_RETURN_VOID();

	ts = my_wait_event_trace;

	write_pos = pg_atomic_read_u64(&ts->write_pos);

	if (write_pos == 0)
		PG_RETURN_VOID();

	/* Read from oldest available to newest */
	read_start = (write_pos > WAIT_EVENT_TRACE_RING_SIZE)
		? write_pos - WAIT_EVENT_TRACE_RING_SIZE : 0;

	for (i = read_start; i < write_pos; i++)
	{
		WaitEventTraceRecord *rec =
			&ts->records[i & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		Datum		values[6];
		bool		nulls[6];
		const char *event_type;
		const char *event_name;
		uint32		seq_before;
		uint32		seq_after;
		uint8		rtype;
		int64		timestamp_ns;
		uint32		event_info;
		int64		duration_ns;
		int64		query_id;

		/* Seqlock read */
		seq_before = rec->seq;
		pg_read_barrier();

		if (seq_before & 1)
			continue;

		rtype = rec->record_type;
		timestamp_ns = rec->timestamp_ns;

		if (rtype == TRACE_WAIT_EVENT)
		{
			event_info = rec->data.wait.event;
			duration_ns = rec->data.wait.duration_ns;
			query_id = 0;
		}
		else if (rtype == TRACE_QUERY_START || rtype == TRACE_QUERY_END ||
				 rtype == TRACE_EXEC_START || rtype == TRACE_EXEC_END)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
		}
		else
		{
			pg_read_barrier();
			continue;
		}

		pg_read_barrier();
		seq_after = rec->seq;

		if (seq_before != seq_after)
			continue;

		/* Skip empty wait events */
		if (rtype == TRACE_WAIT_EVENT && event_info == 0)
			continue;

		if (rtype == TRACE_WAIT_EVENT)
		{
			event_type = pgstat_get_wait_event_type(event_info);
			event_name = pgstat_get_wait_event(event_info);
		}
		else if (rtype == TRACE_QUERY_START)
		{
			event_type = "Query";
			event_name = "QueryStart";
		}
		else if (rtype == TRACE_EXEC_START)
		{
			event_type = "Query";
			event_name = "ExecStart";
		}
		else if (rtype == TRACE_EXEC_END)
		{
			event_type = "Query";
			event_name = "ExecEnd";
		}
		else
		{
			event_type = "Query";
			event_name = "QueryEnd";
		}

		if (event_type == NULL || event_name == NULL)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum((int64) i);
		values[1] = Int64GetDatum(timestamp_ns);
		values[2] = CStringGetTextDatum(event_type);
		values[3] = CStringGetTextDatum(event_name);
		values[4] = Float8GetDatum((double) duration_ns / 1000.0);
		values[5] = Int64GetDatum(query_id);

		tuplestore_putvalues(rsinfo->setResult,
							rsinfo->setDesc,
							values, nulls);
	}

	PG_RETURN_VOID();
}

/*
 * Request a self-reset on the given backend slot.
 *
 * Lock-free: atomically bumps the slot's reset_generation, then sets the
 * target's process latch so an idle backend wakes up and completes its
 * current wait event (which triggers pgstat_report_wait_end_timing, which
 * observes the generation change and performs the reset).  If the target
 * slot is currently unoccupied the SetLatch is a harmless no-op.
 */
static void
wait_event_timing_request_reset(int slot_idx)
{
	Assert(slot_idx >= 0 && slot_idx < NUM_WAIT_EVENT_TIMING_SLOTS);

	/*
	 * If no backend has ever enabled capture, the shared array does not
	 * exist yet -- there is nothing to reset.  Attach read-only; callers
	 * ultimately want the target backend to observe a generation bump,
	 * so if the array isn't allocated the latch set below is also a
	 * harmless no-op (no live backend is tracking).
	 */
	if (!wait_event_timing_attach_array(false))
		return;

	pg_atomic_fetch_add_u32(&WaitEventTimingArray[slot_idx].reset_generation, 1);

	/*
	 * Wake the target if it is sleeping in WaitLatch/WaitEventSetWait so
	 * that it completes its current wait promptly and observes the reset
	 * request.  The slot index is also the PGPROC array index
	 * (pgstat_set_wait_event_timing_storage is called with procNumber).
	 *
	 * Even if no live backend currently owns the slot, setting the latch
	 * on the stale PGPROC is harmless -- latches in shared memory are
	 * durable and no process is waiting on it.
	 */
	if (ProcGlobal != NULL && ProcGlobal->allProcs != NULL)
		SetLatch(&ProcGlobal->allProcs[slot_idx].procLatch);
}

/*
 * SQL function: pg_stat_get_wait_event_timing_overflow()
 *
 * Exposes the per-backend truncation counters that are otherwise
 * write-only: without these, a user has no way to tell from SQL whether
 * their stats are complete or whether the hash table / flat array was
 * saturated mid-session and silently dropped events.
 *
 *   lwlock_overflow_count: number of LWLock wait events that could not
 *       be recorded because the per-backend LWLock timing hash
 *       (LWLOCK_TIMING_MAX_ENTRIES tranches) was full.
 *   flat_overflow_count:   number of non-LWLock wait events that
 *       resolved to an unknown / out-of-range class index and therefore
 *       could not be mapped to a histogram slot.
 *
 * One row per live backend; filtered by HAS_PGSTAT_PERMISSIONS like
 * pg_stat_get_wait_event_timing().
 */
Datum
pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	if (!wait_event_timing_attach_array(false))
		PG_RETURN_VOID();

	for (backend_idx = 0; backend_idx < NUM_WAIT_EVENT_TIMING_SLOTS; backend_idx++)
	{
		WaitEventTimingState *state = &WaitEventTimingArray[backend_idx];
		PgBackendStatus *beentry;
		Datum		values[5];
		bool		nulls[5];

		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			continue;
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(beentry->st_procpid);
		values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
		values[2] = Int32GetDatum(backend_idx + 1);
		values[3] = Int64GetDatum(state->lwlock_overflow_count);
		values[4] = Int64GetDatum(state->flat_overflow_count);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_reset_wait_event_timing(backend_id int4)
 *
 * Resets wait event timing counters.
 *   backend_id = NULL or 0: reset own backend (synchronous)
 *   backend_id > 0:         request reset of a specific backend (superuser)
 *   backend_id = -1:        request reset of ALL backends (superuser)
 *
 * Cross-backend resets are asynchronous by design: the function atomically
 * bumps a per-slot reset_generation counter and wakes the target's latch;
 * the owning backend observes the change on its next wait_end and clears
 * its own counters.  This keeps the hot path lock-free and avoids the
 * cross-writer races that plagued an earlier LWLock-based implementation.
 *
 * Visibility is near-immediate for active backends (the next event ends
 * within microseconds) and is bounded by the target's wait duration for
 * idle backends -- SetLatch shortens that by interrupting any current
 * WaitLatch.  The function returns before the reset has been observed;
 * callers that need strict read-after-reset semantics should either
 * target their own backend (where reset is synchronous) or poll the
 * target until its reset_count increments.
 */
Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	int			target;

	if (PG_ARGISNULL(0) || PG_GETARG_INT32(0) == 0)
	{
		/*
		 * Reset own backend.  Single writer (us), synchronous: no lock or
		 * atomic indirection needed.  We also clear the in-flight wait
		 * fields here because a user resetting their own session has the
		 * clearest intent; any upcoming wait will re-initialise them.
		 *
		 * If capture has never been enabled in this backend yet,
		 * my_wait_event_timing is still NULL; nothing to reset.
		 */
		if (my_wait_event_timing != NULL)
		{
			memset(my_wait_event_timing->events, 0,
				   sizeof(my_wait_event_timing->events));
			memset(&my_wait_event_timing->lwlock_hash, 0,
				   sizeof(LWLockTimingHash));
			my_wait_event_timing->reset_count++;
			my_wait_event_timing->lwlock_overflow_count = 0;
			my_wait_event_timing->flat_overflow_count = 0;
		}
	}
	else
	{
		target = PG_GETARG_INT32(0);

		/* Resetting other backends requires superuser */
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be a superuser to reset other backends' wait event timing")));

		if (target == -1)
		{
			/* Request reset on every slot; owners self-clear on next event */
			for (int i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
				wait_event_timing_request_reset(i);
		}
		else
		{
			int			idx = target - 1;	/* 1-based to 0-based */

			if (idx < 0 || idx >= NUM_WAIT_EVENT_TIMING_SLOTS)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid backend_id: %d", target)));

			wait_event_timing_request_reset(idx);
		}
	}

	PG_RETURN_VOID();
}

#endif							/* USE_WAIT_EVENT_TIMING */
