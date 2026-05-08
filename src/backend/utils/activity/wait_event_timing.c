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
Datum		pg_stat_reset_wait_event_timing_all(PG_FUNCTION_ARGS);

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

Datum
pg_stat_reset_wait_event_timing_all(PG_FUNCTION_ARGS)
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

/*
 * Stub shmem callbacks registered from storage/subsystemlist.h.  In the
 * non-timing build no shared memory is reserved: both request_fn and
 * init_fn are NULL, which RegisterShmemCallbacks() treats as no-ops.
 */
const ShmemCallbacks WaitEventTimingShmemCallbacks = {0};
const ShmemCallbacks WaitEventTraceControlShmemCallbacks = {0};

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
#include "nodes/queryjumble.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
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

/*
 * Same-backend coordination between pg_get_backend_wait_event_trace (the
 * own-session SRF reader) and wait_event_trace_release_slot (the GUC
 * step-down path that frees this backend's ring).  Both paths run in this
 * same backend, single-threaded, so a plain bool is sufficient -- no
 * atomics needed.
 *
 *   srf_in_progress   set true while the SRF is iterating the ring; the
 *                     release path observes this and defers the dsa_free
 *                     instead of yanking the chunk out from under us.
 *
 *   release_pending   set by the release path when it had to defer; the
 *                     SRF's PG_FINALLY checks it and performs the deferred
 *                     dsa_free after the iteration completes.
 *
 * Cross-backend readers (extensions, bgworkers reading another backend's
 * ring) cannot use this mechanism -- they coordinate with the release
 * path via WaitEventTraceCtl->lock instead.  See the header for the
 * recommended snapshot-under-lock pattern for those consumers.
 */
static bool wait_event_trace_srf_in_progress = false;
static bool wait_event_trace_release_pending = false;

/* Forward declarations for lazy-attach helpers */
static void wait_event_timing_ensure_dsa(void);
static bool wait_event_timing_attach_array(bool allocate_if_missing);
static void wait_event_trace_release_slot(int procNumber);

/*
 * Mapping arrays for the flat events[] array, generated from
 * wait_event_names.txt by generate-wait_event_types.pl.
 * Defines: WAIT_EVENT_TIMING_RAW_CLASSES, WAIT_EVENT_TIMING_DENSE_CLASSES,
 *          WAIT_EVENT_TIMING_NUM_EVENTS, and the four mapping arrays.
 */
#include "utils/wait_event_timing_data.h"

/*
 * Convert wait_event_info to a flat index for the events[] array.
 * For bounded classes, eventId equals the array index within the class
 * (the enum values start at PG_WAIT_<CLASS> and increment by one).
 *
 * Class extraction follows the same idiom as pgstat_get_wait_event_type:
 * mask off the class bits and compare against the full PG_WAIT_*
 * constants, rather than shifting both sides down to a byte.  The
 * dense-table lookup still needs the byte-form class id, but that
 * conversion is now an isolated array-index step rather than a
 * load-bearing piece of encoding-layout knowledge in the comparison.
 */
static int
wait_event_timing_index(uint32 wait_event_info)
{
	uint32		classId = wait_event_info & WAIT_EVENT_CLASS_MASK;
	int			eventId = wait_event_info & WAIT_EVENT_ID_MASK;
	int			class_byte;
	int			dense;

	if (classId == PG_WAIT_LWLOCK)
		return WAIT_EVENT_TIMING_IDX_LWLOCK;

	class_byte = classId >> 24;
	if (unlikely(class_byte >= WAIT_EVENT_TIMING_RAW_CLASSES))
		return -1;

	dense = wait_event_class_dense[class_byte];
	if (unlikely(dense < 0))
		return -1;

	if (unlikely(eventId >= wait_event_class_nevents[dense]))
		return -1;

	return wait_event_class_offset[dense] + eventId;
}

/*
 * Reset an LWLockTimingHash to its empty initial state.
 *
 * The DSA region we live in is zero-initialised on allocation, but the
 * empty-slot sentinel is LWLOCK_TIMING_EMPTY_SLOT (0xFFFF), not 0, so
 * we cannot rely on a plain memset(0) for the entries array.  This
 * helper centralises the correct clear sequence -- bulk-zero everything
 * (which initialises num_used and lwlock_events[] correctly), then walk
 * entries[] writing the sentinel.  Every caller that needs to reset or
 * initialise the hash routes through here.
 */
static void
lwlock_timing_hash_clear(LWLockTimingHash *ht)
{
	int		i;

	memset(ht, 0, sizeof(LWLockTimingHash));
	for (i = 0; i < LWLOCK_TIMING_HASH_SIZE; i++)
		ht->entries[i].tranche_id = LWLOCK_TIMING_EMPTY_SLOT;
}

/*
 * Maximum number of probes attempted on the lookup hot path once the
 * table is at capacity (LWLOCK_TIMING_MAX_ENTRIES).  At cap there is
 * no further insertion possible, so an unknown tranche cannot be
 * recorded; the only useful work the loop can do is find an existing
 * entry within its probe-distance window.  Bounding the scan caps the
 * per-event cost at the cap-overflow regime to a constant, instead of
 * paying ~2-3 probes (worst-case clusters: many more) on every
 * unknown-tranche wait_end for the remainder of the backend lifetime.
 *
 * The bound (8) is well above the expected probe distance at this
 * table's load factor of 192/512 = 0.375 (linear-probing miss expected
 * length ~1.78; P99 fits comfortably in 8).  Entries inserted with a
 * collision distance > 8 from their hash slot will fail to be found at
 * cap, which is theoretically possible but astronomically unlikely at
 * 0.375 load (probability < 1e-3) and is the right trade against the
 * common at-cap unknown-tranche cost.
 */
#define LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT 8

/*
 * Look up (or insert) timing entry for an LWLock tranche ID.
 */
static WaitEventTimingEntry *
lwlock_timing_lookup(LWLockTimingHash *ht, uint16 tranche_id)
{
	uint32		hash = (uint32) tranche_id * 2654435761U;
	int			slot = hash & (LWLOCK_TIMING_HASH_SIZE - 1);
	int			limit;
	int			i;

	/*
	 * At cap, bound the probe distance so unknown tranches return NULL
	 * quickly instead of walking through clustered occupied slots.  See
	 * the comment on LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT.
	 */
	limit = (ht->num_used >= LWLOCK_TIMING_MAX_ENTRIES)
		? LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT
		: LWLOCK_TIMING_HASH_SIZE;

	for (i = 0; i < limit; i++)
	{
		LWLockTimingHashEntry *e = &ht->entries[slot];

		if (e->tranche_id == tranche_id)
			return &ht->lwlock_events[e->dense_idx];

		if (e->tranche_id == LWLOCK_TIMING_EMPTY_SLOT)
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
 *
 * Bin edges are powers of two directly on nanoseconds: bucket i covers
 * [2^(i+9), 2^(i+10)) ns for 0 < i < NBUCKETS-1, bucket 0 covers
 * [0, 1024) ns, and the last bucket covers [2^(NBUCKETS+8), inf) ns.
 * The boundaries approximate the decimal-microsecond grid (1024 ≈ 1 us,
 * 2048 ≈ 2 us, ... 2^24 ≈ 16 ms) close enough for a diagnostic histogram
 * while letting us skip the strength-reduced /1000 on the hot path.
 */
static int
wait_event_timing_bucket(int64 duration_ns)
{
	int			bucket;

	/*
	 * Everything under 1024 ns ("~1 us") lands in bucket 0.  Also handles
	 * duration_ns == 0, which would otherwise be undefined input to
	 * pg_leftmost_one_pos64.
	 */
	if (duration_ns < 1024)
		return 0;

	bucket = pg_leftmost_one_pos64((uint64) duration_ns) - 9;

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

	/*
	 * Claim the next slot.  Single-writer counter (only the owning backend
	 * writes its own ring), so a plain read+write is sufficient and avoids
	 * the LOCK XADD that pg_atomic_fetch_add_u64 would emit -- a wasted
	 * cache-coherence trip on an unshared cache line at this rate (one per
	 * wait event).  Cross-backend readers use pg_atomic_read_u64, which
	 * compiles to a plain MOV on x86 and tolerates concurrent writes here
	 * (their actual safety against the records[] window is the per-record
	 * seqlock below).  Same idiom as injection_point.c's per-entry
	 * generation counter (single writer + multiple lock-free readers).
	 */
	pos = pg_atomic_read_u64(&my_wait_event_trace->write_pos);
	pg_atomic_write_u64(&my_wait_event_trace->write_pos, pos + 1);
	rec = &my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
	seq = (uint32)(pos * 2 + 1);

	rec->seq = seq;
	pg_write_barrier();		/* release: payload stores must not rise above seq=odd */

	INSTR_TIME_SET_CURRENT(now);
	rec->record_type = record_type;
	rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
	rec->data.query.query_id = query_id;
	rec->data.query.pad2 = 0;

	pg_write_barrier();		/* release: payload stores must land before seq=even */
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
 * Report and initialize shared memory for wait event timing.
 *
 * Registered via the shmem subsystem registry in
 * src/include/storage/subsystemlist.h.  Only the small control struct
 * is in fixed shmem; the per-backend WaitEventTimingState array
 * (~30 KB/backend) is allocated lazily in DSA on first enable by any
 * backend (see wait_event_timing_attach_array).
 */
static void
WaitEventTimingShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "WaitEventTimingControl",
					   .size = sizeof(WaitEventTimingControl),
					   .ptr = (void **) &WaitEventTimingCtl);
}

static void
WaitEventTimingShmemInit(void *arg)
{
	LWLockInitialize(&WaitEventTimingCtl->lock,
					 LWTRANCHE_WAIT_EVENT_TIMING_DSA);
	WaitEventTimingCtl->timing_dsa_handle = DSA_HANDLE_INVALID;
	WaitEventTimingCtl->timing_array = InvalidDsaPointer;

	WaitEventTimingArray = NULL;
}

const ShmemCallbacks WaitEventTimingShmemCallbacks = {
	.request_fn = WaitEventTimingShmemRequest,
	.init_fn = WaitEventTimingShmemInit,
};

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
 * Re-entrancy guard.  Internal operations below (dsa_create,
 * dsa_allocate_extended, the LWLockAcquire inside ensure_dsa) can
 * emit LWLock wait events of their own, which feed into the wait-end
 * timing hot path; under wait_event_capture >= STATS that hot path
 * lazy-attaches by calling back into this function.  Without the
 * guard we would either deadlock on WaitEventTimingCtl->lock or
 * recurse with a half-initialised slot pointer.
 *
 * The same hazard applies in wait_event_trace_attach (which also runs
 * dsa_allocate / LWLock under its body) and in
 * wait_event_trace_release_slot (whose dsa_free takes a DSA-internal
 * LWLock that can in principle emit a wait event during shutdown
 * sequences).  Each function carries its own static bool guard close
 * to the code it protects, matching the established PG idiom for
 * function-local re-entry guards (see, e.g., in_vacuum in
 * src/backend/commands/vacuum.c, in_streamed_transaction in
 * src/backend/replication/logical/worker.c).  We deliberately do NOT
 * collapse these into a shared bitmask because:
 *   1. PG style places re-entry flags adjacent to the function they
 *      protect, not in a centralised module-level state structure.
 *   2. The three guarded functions are independent: a re-entry into
 *      one of them while another is in flight is a legitimate pattern
 *      (e.g., release_slot can be triggered by an assign hook that
 *      itself ran while attach was in progress earlier).  A shared
 *      flag would conservatively block those legal cases.
 *
 * If you add a fourth re-entrant function in this file, follow the
 * same shape: a `static bool in_<verb> = false;` at the top of the
 * function, an early-return `if (in_<verb>) return ...;`, set true
 * before the body, clear in PG_FINALLY so an ereport(ERROR) cannot
 * leave the flag stuck set.
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
	lwlock_timing_hash_clear(&my_wait_event_timing->lwlock_hash);
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
static Size
WaitEventTraceControlShmemSize(void)
{
	return add_size(offsetof(WaitEventTraceControl, trace_ptrs),
					mul_size(NUM_WAIT_EVENT_TIMING_SLOTS, sizeof(dsa_pointer)));
}

static void
WaitEventTraceControlShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "WaitEventTraceControl",
					   .size = WaitEventTraceControlShmemSize(),
					   .ptr = (void **) &WaitEventTraceCtl);
}

/*
 * Initialize shared memory for trace ring buffer control.
 */
static void
WaitEventTraceControlShmemInit(void *arg)
{
	int		i;

	WaitEventTraceCtl->trace_dsa_handle = DSA_HANDLE_INVALID;
	LWLockInitialize(&WaitEventTraceCtl->lock,
					 LWTRANCHE_WAIT_EVENT_TRACE_DSA);
	for (i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
		WaitEventTraceCtl->trace_ptrs[i] = InvalidDsaPointer;
}

const ShmemCallbacks WaitEventTraceControlShmemCallbacks = {
	.request_fn = WaitEventTraceControlShmemRequest,
	.init_fn = WaitEventTraceControlShmemInit,
};

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
	 * Re-entrancy guard.  dsa_create / dsa_allocate_extended below can
	 * emit wait events internally; those reach the lazy-attach hot path
	 * which calls back into this function while we still hold
	 * WaitEventTraceCtl->lock or are mid-allocation.  See the
	 * function-local-static-bool pattern explainer on
	 * wait_event_timing_attach_array.
	 */
	static bool in_attach = false;
	static bool shmem_exit_registered = false;
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
			 * Guarded by shmem_exit_registered because under the
			 * release-on-disable policy (see wait_event_trace_release_slot
			 * and assign_wait_event_capture) the allocate branch can run
			 * multiple times per backend lifetime -- once per
			 * off/stats -> trace re-enable cycle.  The cleanup itself is
			 * idempotent (it short-circuits when trace_ptrs[procNumber]
			 * is InvalidDsaPointer), but we avoid growing the
			 * before_shmem_exit callback list.
			 */
			if (!shmem_exit_registered)
			{
				before_shmem_exit(wait_event_trace_before_shmem_exit,
								  Int32GetDatum(procNumber));
				shmem_exit_registered = true;
			}
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
 * Release this backend's trace ring buffer back to DSA immediately.
 *
 * Called from assign_wait_event_capture when the user steps down from
 * TRACE to STATS or OFF.  Without this, a ~4 MB ring allocated by a
 * brief investigation would remain pinned for the rest of the session's
 * lifetime, which can leak gigabytes across large connection pools.
 *
 * The operation is LWLock-safe and does not raise -- dsa_free is pure
 * bookkeeping on the DSA freelist, no allocation and no ereport paths.
 * Safe to call from a GUC assign hook.  If the ring is later re-attached
 * (user re-enables TRACE), wait_event_trace_attach takes the "allocate"
 * branch again and a fresh ring is created on first wait event.
 *
 * If pg_get_backend_wait_event_trace is currently iterating our own ring
 * (wait_event_trace_srf_in_progress), we must NOT free the chunk out
 * from under it: that would be a use-after-free on the records[] the SRF
 * is still reading.  Set wait_event_trace_release_pending instead and
 * return; the SRF's PG_FINALLY block will perform the deferred free
 * after iteration completes.  In practice this branch is unreachable in
 * current PG (assign hooks fire only at command boundaries and the SRF
 * is a single command), but it makes the invariant explicit and the
 * future-proofing free.
 */
static void
wait_event_trace_release_slot(int procNumber)
{
	/*
	 * Re-entrancy guard.  dsa_free takes a DSA-internal LWLock which can
	 * in principle emit a wait event; if a nested assign hook re-enters
	 * we must not recurse.  See the function-local-static-bool pattern
	 * explainer on wait_event_timing_attach_array.
	 */
	static bool in_release = false;

	if (in_release)
		return;

	if (WaitEventTraceCtl == NULL || trace_dsa == NULL)
		return;

	/*
	 * Same-backend SRF is iterating our own ring.  Defer the free until
	 * the SRF's PG_FINALLY runs.
	 */
	if (wait_event_trace_srf_in_progress)
	{
		wait_event_trace_release_pending = true;
		return;
	}

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	in_release = true;
	PG_TRY();
	{
		LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);
		if (DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[procNumber]))
		{
			dsa_free(trace_dsa, WaitEventTraceCtl->trace_ptrs[procNumber]);
			WaitEventTraceCtl->trace_ptrs[procNumber] = InvalidDsaPointer;
		}
		LWLockRelease(&WaitEventTraceCtl->lock);

		my_wait_event_trace = NULL;
	}
	PG_FINALLY();
	{
		in_release = false;
	}
	PG_END_TRY();
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
 * Three responsibilities, all correctness- or resource-critical:
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
 * 2) Release the trace ring buffer when stepping down from TRACE.
 *    The per-backend trace ring is ~4 MB of DSA memory, and leaving it
 *    pinned for the rest of the session's lifetime leaks shmem across
 *    large connection pools that briefly sample trace.  Freeing here
 *    makes "wait_event_capture = off" semantically release resources.
 *    The next re-enable re-allocates a fresh ring on first wait event
 *    via wait_event_trace_attach.
 *
 * 3) Warn (but never error) about secondary preconditions for TRACE
 *    level.  GUC assign hooks MUST NOT ereport(ERROR) -- see
 *    src/backend/utils/misc/README -- because they can run during
 *    transaction rollback when lookups are unsafe.  In particular, the
 *    trace ring's DSA allocation is NOT performed here (it can raise on
 *    OOM).  Instead, the ring is attached lazily on the first write
 *    from wait_event_trace_write_marker() and
 *    pgstat_report_wait_end_timing(), where ereport(ERROR) has
 *    well-defined semantics.  The release path above is safe to call
 *    from the hook because dsa_free is non-raising LWLock bookkeeping.
 */
void
assign_wait_event_capture(int newval, void *extra)
{
	if (my_wait_event_timing != NULL)
	{
		INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
		my_wait_event_timing->current_event = 0;
	}

	/*
	 * Step-down from TRACE: release the ring now instead of at backend
	 * exit.  Only fires when a ring is actually attached, so going
	 * directly OFF -> TRACE -> OFF without ever having emitted a trace
	 * record is still a no-op.
	 */
	if (newval != WAIT_EVENT_CAPTURE_TRACE && my_wait_event_trace != NULL)
		wait_event_trace_release_slot(my_trace_proc_number);

	if (newval == WAIT_EVENT_CAPTURE_TRACE && !pgstat_track_activities)
		ereport(WARNING,
				(errmsg("wait_event_capture = trace query attribution "
						"requires track_activities to be enabled")));

	if (newval == WAIT_EVENT_CAPTURE_TRACE &&
		compute_query_id == COMPUTE_QUERY_ID_OFF)
		ereport(WARNING,
				(errmsg("wait_event_capture = trace query attribution "
						"requires compute_query_id to be enabled"),
				 errhint("Set compute_query_id to \"on\" or \"auto\", or "
						 "load an extension that enables it (e.g. "
						 "pg_stat_statements).")));
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
 *
 * The capture_level argument is the value of wait_event_capture as
 * observed at the inline gate.  Passing it through (rather than
 * re-loading the global here) avoids a redundant memory load on the
 * trace hot path: the function-call boundary defeats CSE, so without
 * the parameter the compiler must emit a second load to test for
 * TRACE level below.  Using the gate's view also means a concurrent
 * GUC change cannot half-update this call -- we either ran in the
 * old level or we don't run at all.
 */
void
pgstat_report_wait_end_timing(int capture_level)
{
	uint32		event = my_wait_event_timing->current_event;
	uint32		cur_reset_gen;

	/*
	 * Fast check for a pending cross-backend reset request.  Single
	 * atomic load; almost always hits the fast path (branch well
	 * predicted).  When we detect that our shared reset_generation has
	 * advanced, clear our own counters on behalf of the requester, then
	 * continue with normal accumulation.  wait_start is deliberately
	 * left untouched so we don't lose the measurement that's already
	 * running; the completing event will land in the freshly-zeroed
	 * counters, which is the desired behaviour.  current_event is safe
	 * to zero here because the local "event" above already captured its
	 * value before the reset block; zeroing it kills a source of stale
	 * state that external readers would otherwise observe on the slot
	 * between waits.
	 */
	cur_reset_gen = pg_atomic_read_u32(&my_wait_event_timing->reset_generation);
	if (unlikely(cur_reset_gen != my_last_reset_generation))
	{
		memset(my_wait_event_timing->events, 0,
			   sizeof(my_wait_event_timing->events));
		lwlock_timing_hash_clear(&my_wait_event_timing->lwlock_hash);
		my_wait_event_timing->reset_count++;
		my_wait_event_timing->lwlock_overflow_count = 0;
		my_wait_event_timing->flat_overflow_count = 0;
		my_wait_event_timing->current_event = 0;
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
		if (unlikely(capture_level == WAIT_EVENT_CAPTURE_TRACE))
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
				/*
				 * Single-writer claim: read+write avoids the LOCK XADD that
				 * pg_atomic_fetch_add_u64 would emit on every wait event.
				 * See wait_event_trace_write_marker for the full rationale.
				 */
				uint64	pos = pg_atomic_read_u64(&my_wait_event_trace->write_pos);
				WaitEventTraceRecord *rec;
				uint32	seq;

				pg_atomic_write_u64(&my_wait_event_trace->write_pos, pos + 1);
				rec = &my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
				seq = (uint32)(pos * 2 + 1);

				rec->seq = seq;
				pg_write_barrier();		/* release: payload stores must not rise above seq=odd */

				rec->record_type = TRACE_WAIT_EVENT;
				rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
				rec->data.wait.event = event;
				rec->data.wait.pad2 = 0;
				rec->data.wait.duration_ns = duration_ns;

				pg_write_barrier();		/* release: payload stores must land before seq=even */
				rec->seq = seq + 1;
			}
		}

		INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
	}
}

/*
/*
 * Resolve the optional pid SRF argument to a procNumber range
 * [out_start, out_end).  Returns true on success, false if the SRF
 * should emit zero rows (unknown pid -- silent no-op, matching the
 * pg_stat_reset_wait_event_timing convention).
 *
 *   PID NULL  -> sweep all NUM_WAIT_EVENT_TIMING_SLOTS slots.
 *   PID known -> sweep the single slot belonging to that backend.
 *   PID unknown / invalid -> emit no rows.
 */
static bool
wait_event_timing_pid_range(FunctionCallInfo fcinfo,
							int *out_start, int *out_end)
{
	if (PG_ARGISNULL(0))
	{
		*out_start = 0;
		*out_end = NUM_WAIT_EVENT_TIMING_SLOTS;
		return true;
	}
	else
	{
		int		target_pid = PG_GETARG_INT32(0);
		PGPROC *proc;
		int		procNumber;

		proc = BackendPidGetProc(target_pid);
		if (proc == NULL)
			proc = AuxiliaryPidGetProc(target_pid);
		if (proc == NULL)
			return false;

		procNumber = GetNumberFromPGProc(proc);
		if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
			return false;

		*out_start = procNumber;
		*out_end = procNumber + 1;
		return true;
	}
}

/*
 * SQL function: pg_stat_get_wait_event_timing(pid int4, OUT ...)
 *
 * Returns one row per (backend, wait_event) with non-zero counts.
 * pid is optional: NULL means all backends; a non-NULL value restricts
 * the sweep to that single backend (silently empty if the PID is
 * unknown, matching pg_stat_reset_wait_event_timing(pid) semantics).
 *
 * The PID-filtered fast path turns the cost of cluster-wide monitoring
 * loops that poll a specific PID from O(MaxBackends * events) into
 * O(events) per call -- the same precedent as pg_stat_get_activity(pid).
 *
 * Uses InitMaterializedSRF (materialize-all) for simplicity.  The result
 * set is bounded by (NUM_WAIT_EVENT_TIMING_SLOTS * WAIT_EVENT_TIMING_NUM_EVENTS)
 * rows, so deferred (value-per-call) mode is not needed.
 */
Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			start_idx;
	int			end_idx;
	int			backend_idx;
	ArrayType  *hist_array;
	int64	   *hist_payload;

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If no backend has ever enabled wait_event_capture since the last
	 * postmaster start, the shared timing array has not been allocated
	 * yet -- return zero rows rather than forcing an allocation just for
	 * a read.
	 */
	if (!wait_event_timing_attach_array(false))
		PG_RETURN_VOID();

	if (!wait_event_timing_pid_range(fcinfo, &start_idx, &end_idx))
		PG_RETURN_VOID();

	/*
	 * Allocate the histogram ArrayType once and reuse it across every row
	 * emitted below.  Per-row we overwrite the 16 int8 payload slots via
	 * ARR_DATA_PTR; tuplestore_putvalues flattens the varlena into its
	 * stored tuple, so subsequent rewrites cannot corrupt previously
	 * emitted rows.  Saves one palloc per row on SRFs that can easily
	 * produce tens of thousands of rows on large clusters.
	 */
	{
		Datum		zero_elems[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];

		memset(zero_elems, 0, sizeof(zero_elems));
		hist_array = construct_array_builtin(zero_elems,
											 WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS,
											 INT8OID);
		hist_payload = (int64 *) ARR_DATA_PTR(hist_array);
	}

	for (backend_idx = start_idx; backend_idx < end_idx; backend_idx++)
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

				for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
					hist_payload[bucket] = entry->histogram[bucket];
				values[9] = PointerGetDatum(hist_array);

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

			if (he->tranche_id == LWLOCK_TIMING_EMPTY_SLOT)
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

			for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
				hist_payload[bucket] = entry->histogram[bucket];
			values[9] = PointerGetDatum(hist_array);

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
 * workers).  The recommended cross-backend reader pattern is documented
 * on WaitEventTraceControl in wait_event_timing.h.  The name mirrors
 * pg_get_backend_memory_contexts() to make the session-local scope
 * explicit at the API level.
 *
 * Same-backend coordination with wait_event_trace_release_slot uses the
 * wait_event_trace_srf_in_progress / _release_pending flags rather than
 * an LWLock: same-backend serialization is implicit, so a per-backend
 * bool plus a deferred-free path is sufficient and avoids any of the
 * cross-backend lock-hold latency that the cross-backend reader pattern
 * has to manage.  PG_TRY/PG_FINALLY guarantees the flag is cleared and
 * any deferred dsa_free is performed even on ereport(ERROR).
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

	/*
	 * Mark the iteration in progress so wait_event_trace_release_slot
	 * defers any concurrent dsa_free of our own ring (see the comment on
	 * that function for the deferral protocol).  PG_FINALLY clears the
	 * flag and performs any deferred free, even on ereport(ERROR).
	 */
	wait_event_trace_srf_in_progress = true;
	PG_TRY();
	{
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
		pg_read_barrier();		/* acquire: payload loads below must not rise above this */

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
			pg_read_barrier();	/* acquire: pair with seq_before read above before skipping */
			continue;
		}

		pg_read_barrier();		/* acquire: payload loads must have landed before seq_after */
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
	}
	PG_FINALLY();
	{
		wait_event_trace_srf_in_progress = false;

		/*
		 * If a GUC step-down fired during iteration, it deferred the
		 * dsa_free.  Process it now that we're safely past the loop.
		 * Re-check release_pending under the same flag to handle the
		 * (impossible-today, possible-tomorrow) case of a nested SRF.
		 */
		if (wait_event_trace_release_pending)
		{
			wait_event_trace_release_pending = false;
			if (my_trace_proc_number >= 0)
				wait_event_trace_release_slot(my_trace_proc_number);
		}
	}
	PG_END_TRY();

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
 *   reset_count:           number of times this backend's counters have
 *       been reset (own reset or cross-backend request observed).
 *       Callers of pg_stat_reset_wait_event_timing(target) can poll this
 *       column to wait until an asynchronous reset has taken effect.
 *
 * One row per live backend; filtered by HAS_PGSTAT_PERMISSIONS like
 * pg_stat_get_wait_event_timing().  The pid argument is optional with
 * the same semantics as pg_stat_get_wait_event_timing(): NULL means
 * all backends, a non-NULL value restricts the sweep to that single
 * backend (silently empty for unknown PIDs).
 */
Datum
pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			start_idx;
	int			end_idx;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	if (!wait_event_timing_attach_array(false))
		PG_RETURN_VOID();

	if (!wait_event_timing_pid_range(fcinfo, &start_idx, &end_idx))
		PG_RETURN_VOID();

	for (backend_idx = start_idx; backend_idx < end_idx; backend_idx++)
	{
		WaitEventTimingState *state = &WaitEventTimingArray[backend_idx];
		PgBackendStatus *beentry;
		Datum		values[6];
		bool		nulls[6];

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
		values[5] = Int64GetDatum(state->reset_count);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_reset_wait_event_timing(pid int4)
 *
 * Resets wait-event-timing counters for a single backend, identified by PID.
 *
 *   NULL (or MyProcPid): reset caller's own session synchronously --
 *                        single writer, no lock needed.
 *   another PID:         request a cross-backend reset (superuser only).
 *   unknown / dead PID:  silent no-op, matching pg_stat_reset_backend_stats.
 *
 * To reset every backend, use pg_stat_reset_wait_event_timing_all().
 *
 * Cross-backend resets are asynchronous by design: the function atomically
 * bumps the target slot's reset_generation counter and wakes the target's
 * latch; the owning backend observes the change on its next wait_end and
 * clears its own counters.  This keeps the hot path lock-free and avoids
 * the cross-writer races that plagued an earlier LWLock-based design.
 *
 * Visibility is near-immediate for active backends (their next event ends
 * within microseconds) and is bounded by the target's wait duration for
 * idle backends -- SetLatch shortens that by interrupting any current
 * WaitLatch.  The function returns before the reset has been observed;
 * callers that need strict read-after-reset semantics should either
 * target their own backend (where reset is synchronous) or poll the
 * target's reset_count column in pg_stat_wait_event_timing_overflow
 * until it increments.
 */
Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	int			target_pid;
	PGPROC	   *proc;
	int			procNumber;

	if (PG_ARGISNULL(0) || PG_GETARG_INT32(0) == MyProcPid)
	{
		/*
		 * Reset own backend.  Synchronous: no lock or atomic indirection
		 * needed.  If capture has never been enabled in this backend yet,
		 * my_wait_event_timing is still NULL; nothing to reset.
		 *
		 * wait_start is already zero here -- pgstat_report_wait_end_timing
		 * zeros it at the end of every wait, and the backend cannot be mid-
		 * wait while it is executing this SQL function -- so there is no
		 * in-flight measurement to preserve.  We zero current_event for the
		 * same hygiene reason as the cross-backend reset path above: keep
		 * external readers of the slot from seeing stale state between
		 * waits.
		 */
		if (my_wait_event_timing != NULL)
		{
			memset(my_wait_event_timing->events, 0,
				   sizeof(my_wait_event_timing->events));
			lwlock_timing_hash_clear(&my_wait_event_timing->lwlock_hash);
			my_wait_event_timing->reset_count++;
			my_wait_event_timing->lwlock_overflow_count = 0;
			my_wait_event_timing->flat_overflow_count = 0;
			my_wait_event_timing->current_event = 0;
		}
		PG_RETURN_VOID();
	}

	/* Resetting other backends requires superuser */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to reset other backends' wait event timing")));

	target_pid = PG_GETARG_INT32(0);

	/* Look up the target.  Try regular backends first, then aux. */
	proc = BackendPidGetProc(target_pid);
	if (proc == NULL)
		proc = AuxiliaryPidGetProc(target_pid);

	/* Unknown / dead PID: silent no-op, matching pg_stat_reset_backend_stats. */
	if (proc == NULL)
		PG_RETURN_VOID();

	procNumber = GetNumberFromPGProc(proc);

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		PG_RETURN_VOID();

	wait_event_timing_request_reset(procNumber);

	PG_RETURN_VOID();
}

/*
 * Reset wait-event-timing counters for every backend.  Superuser only.
 *
 * Each slot is asked to self-reset on its next wait event (owner-cleared);
 * see wait_event_timing_request_reset for the protocol.  Returns before the
 * resets have been observed -- callers that need strict read-after-reset
 * semantics should poll the targets' reset_count columns.
 */
Datum
pg_stat_reset_wait_event_timing_all(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to reset wait event timing for all backends")));

	for (int i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
		wait_event_timing_request_reset(i);

	PG_RETURN_VOID();
}

#endif							/* USE_WAIT_EVENT_TIMING */
