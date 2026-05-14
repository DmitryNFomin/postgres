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
 * GUC: cap on distinct LWLock tranches the per-backend hash table
 * tracks individually.  Sized at server start (PGC_POSTMASTER).  See
 * the description in guc_parameters.dat.  Always defined so the GUC
 * machinery has a backing variable even on builds compiled without
 * --enable-wait-event-timing; the value is unused outside that gate.
 */
int			wait_event_timing_max_tranches = 192;

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
Datum		pg_get_wait_event_trace(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS);
Datum		pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS);
Datum		pg_stat_reset_wait_event_timing_all(PG_FUNCTION_ARGS);
Datum		pg_stat_clear_orphaned_wait_event_rings(PG_FUNCTION_ARGS);

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
pg_get_wait_event_trace(PG_FUNCTION_ARGS)
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

Datum
pg_stat_clear_orphaned_wait_event_rings(PG_FUNCTION_ARGS)
{
	/*
	 * In stub builds the trace ring infrastructure does not exist, so
	 * there can never be any orphaned rings to clear.  Return 0 rather
	 * than erroring; this lets monitoring scripts call the function
	 * unconditionally without branching on the build flag.
	 */
	PG_RETURN_INT64(0);
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
#include "utils/injection_point.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

#define NUM_WAIT_EVENT_TIMING_SLOTS  (MaxBackends + NUM_AUXILIARY_PROCS)

#define HAS_PGSTAT_PERMISSIONS(role) \
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

/* Pointer to this backend's timing state */
WaitEventTimingState *my_wait_event_timing = NULL;

/* Pointer to this backend's trace ring buffer */
static WaitEventTraceState *my_wait_event_trace = NULL;

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
 * Backend-local cached pointer to the start of the shared array, set
 * on first lazy-attach.  Readers of other backends' slots (pg_stat_*)
 * attach on demand and use this cache for the rest of the SRF call.
 * Writers access their own slot exclusively via my_wait_event_timing.
 *
 * Slots in this region are NOT laid out as a simple C array -- per
 * the layout description on WaitEventTimingState (in
 * src/include/utils/wait_event_timing.h), each slot has a
 * runtime-determined stride (header + variable-size hash arrays).
 * Use wet_slot(idx) below to index into it.
 */
static char *WaitEventTimingArray = NULL;

/*
 * Per-backend slot stride within WaitEventTimingArray.  Set at first
 * attach from the GUC value at the time of allocation; constant for
 * the cluster's lifetime once the DSA is allocated.
 */
static Size wait_event_timing_per_backend_stride = 0;

/*
 * Effective hash sizing.  Both values are derived from the GUC
 * wait_event_timing_max_tranches at allocation time and stored in
 * each slot's LWLockTimingHash header; cached here as backend-local
 * for use by code that needs the values before resolving a slot
 * (e.g., the allocation code itself).
 */
static int	wait_event_timing_hash_size = 0;
static int	wait_event_timing_max_entries = 0;

/*
 * Round up to the next power of two, with a minimum of 32.  The hash
 * slot count must be a power of two for the mask-based modulo in the
 * lookup hot path; we target >= 2x the entry cap so the load factor
 * stays at or below 50%.
 */
static int
wait_event_timing_hash_size_for(int max_entries)
{
	int		size = 32;

	while (size < max_entries * 2)
		size <<= 1;
	return size;
}

/*
 * Compute the per-backend slot size for the given max_entries.  Each
 * slot is laid out as
 *
 *     [ WaitEventTimingState header ]
 *     [ LWLockTimingHashEntry[hash_size] ]
 *     [ WaitEventTimingEntry[max_entries]    <- lwlock_events[] ]
 *
 * with no padding between sections (the structs already pack
 * 8-byte-aligned).
 */
static Size
wait_event_timing_slot_size(int max_entries)
{
	int		hash_size = wait_event_timing_hash_size_for(max_entries);

	return add_size(sizeof(WaitEventTimingState),
					add_size(mul_size(hash_size, sizeof(LWLockTimingHashEntry)),
							 mul_size(max_entries, sizeof(WaitEventTimingEntry))));
}

/* Resolve the address of slot `idx` within WaitEventTimingArray. */
static inline WaitEventTimingState *
wet_slot(int idx)
{
	return (WaitEventTimingState *)
		(WaitEventTimingArray + (Size) idx * wait_event_timing_per_backend_stride);
}

/*
 * Address of the LWLock hash slot table for the given slot's lwlock_hash
 * header.  The slot table immediately follows the WaitEventTimingState
 * header in memory; hash_size in the LWLockTimingHash header tells us
 * how many entries follow.
 */
static inline LWLockTimingHashEntry *
wet_lwlock_hash_entries(WaitEventTimingState *state)
{
	return (LWLockTimingHashEntry *)((char *) state + sizeof(WaitEventTimingState));
}

/*
 * Address of the dense LWLock events array for the given slot.  It
 * immediately follows the slot table.
 */
static inline WaitEventTimingEntry *
wet_lwlock_hash_events(WaitEventTimingState *state)
{
	return (WaitEventTimingEntry *)
		((char *) state + sizeof(WaitEventTimingState)
		 + (Size) state->lwlock_hash.hash_size * sizeof(LWLockTimingHashEntry));
}

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
 * Reset a slot's LWLockTimingHash to its empty initial state.
 *
 * Takes a WaitEventTimingState rather than a bare LWLockTimingHash
 * because the slot table (entries[]) and dense events array
 * (lwlock_events[]) live as variable-size regions following the
 * WaitEventTimingState header in memory; their sizes are runtime-
 * determined by wait_event_timing_max_tranches.  The hash header's
 * hash_size and max_entries fields are immutable after allocation
 * and are NOT reset here.
 */
static void
lwlock_timing_hash_clear(WaitEventTimingState *state)
{
	LWLockTimingHash *ht = &state->lwlock_hash;
	LWLockTimingHashEntry *entries = wet_lwlock_hash_entries(state);
	WaitEventTimingEntry *events = wet_lwlock_hash_events(state);
	int			i;

	ht->num_used = 0;
	memset(events, 0, (Size) ht->max_entries * sizeof(WaitEventTimingEntry));
	for (i = 0; i < ht->hash_size; i++)
	{
		entries[i].tranche_id = LWLOCK_TIMING_EMPTY_SLOT;
		entries[i].dense_idx = 0;
	}
}

/*
 * Maximum number of probes attempted on the lookup hot path once the
 * table is at capacity.  At cap there is no further insertion
 * possible, so an unknown tranche cannot be recorded; the only useful
 * work the loop can do is find an existing entry within its
 * probe-distance window.  Bounding the scan caps the per-event cost at
 * the cap-overflow regime to a constant, instead of paying ~2-3 probes
 * (worst-case clusters: many more) on every unknown-tranche wait_end
 * for the remainder of the backend lifetime.
 *
 * The bound (8) is well above the expected probe distance at this
 * table's load factor (linear-probing miss expected length ~1.78 at
 * 37.5% load; P99 fits comfortably in 8).  Entries inserted with a
 * collision distance > 8 from their hash slot will fail to be found at
 * cap, which is theoretically possible but astronomically unlikely at
 * the load factors we target (probability < 1e-3) and is the right
 * trade against the common at-cap unknown-tranche cost.
 */
#define LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT 8

/*
 * Look up (or insert) timing entry for an LWLock tranche ID.
 *
 * Takes WaitEventTimingState (rather than just the hash header) so the
 * variable-size entries[] and lwlock_events[] arrays following the
 * header can be addressed via the wet_lwlock_hash_*() helpers.
 */
static WaitEventTimingEntry *
lwlock_timing_lookup(WaitEventTimingState *state, uint16 tranche_id)
{
	LWLockTimingHash *ht = &state->lwlock_hash;
	LWLockTimingHashEntry *entries = wet_lwlock_hash_entries(state);
	WaitEventTimingEntry *events = wet_lwlock_hash_events(state);
	uint32		hash = (uint32) tranche_id * 2654435761U;
	int			slot = hash & (ht->hash_size - 1);
	int			limit;
	int			i;

	/*
	 * At cap, bound the probe distance so unknown tranches return NULL
	 * quickly instead of walking through clustered occupied slots.  See
	 * the comment on LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT.
	 */
	limit = (ht->num_used >= ht->max_entries)
		? LWLOCK_TIMING_LOOKUP_AT_CAP_PROBE_LIMIT
		: ht->hash_size;

	for (i = 0; i < limit; i++)
	{
		LWLockTimingHashEntry *e = &entries[slot];

		if (e->tranche_id == tranche_id)
			return &events[e->dense_idx];

		if (e->tranche_id == LWLOCK_TIMING_EMPTY_SLOT)
		{
			if (ht->num_used >= ht->max_entries)
				return NULL;

			e->tranche_id = tranche_id;
			e->dense_idx = ht->num_used++;
			return &events[e->dense_idx];
		}

		slot = (slot + 1) & (ht->hash_size - 1);
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
 * 2048 ≈ 2 us, ... 2^33 ≈ 8.6 s) close enough for a diagnostic
 * histogram while letting us skip the strength-reduced /1000 on the
 * hot path.
 *
 * See the rationale comment on WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS in
 * wait_event_timing.h for why the bucket count is 32 (covering up to
 * 8.6s) rather than 16 (which would have capped at 16ms).
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
	 *
	 * No likely()/unlikely() annotation: this function is called at
	 * query/exec boundaries (a handful per query, not per wait event),
	 * so neither side of the branch dominates often enough for static
	 * layout to matter, and the meaningful production configuration
	 * (wait_event_capture = trace) is exactly when the body is hot --
	 * an annotation on the early-return would point the wrong way.
	 */
	if (wait_event_capture != WAIT_EVENT_CAPTURE_TRACE || query_id == 0)
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
				int		max_entries;
				int		hash_size;
				Size	stride;
				Size	total;

				/*
				 * Snapshot the GUC at allocation time and use the same
				 * value for every slot in the cluster.  This is the
				 * cluster-wide first-enable allocation; subsequent
				 * backends that attach reuse these dimensions, even if
				 * the GUC has somehow been changed in between (it
				 * shouldn't, since it is PGC_POSTMASTER, but reading
				 * once and storing the result keeps the contract
				 * explicit).
				 */
				max_entries = wait_event_timing_max_tranches;
				hash_size = wait_event_timing_hash_size_for(max_entries);
				stride = wait_event_timing_slot_size(max_entries);
				total = mul_size(NUM_WAIT_EVENT_TIMING_SLOTS, stride);

				LWLockAcquire(&WaitEventTimingCtl->lock, LW_EXCLUSIVE);

				if (WaitEventTimingCtl->timing_array == InvalidDsaPointer)
				{
					dsa_pointer p;
					char	   *region;
					int			i;

					p = dsa_allocate_extended(timing_dsa, total,
											  DSA_ALLOC_ZERO);
					region = (char *) dsa_get_address(timing_dsa, p);

					for (i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
					{
						WaitEventTimingState *slot;
						LWLockTimingHashEntry *slot_entries;
						int			j;

						slot = (WaitEventTimingState *) (region + (Size) i * stride);

						pg_atomic_init_u32(&slot->reset_generation, 0);
						slot->lwlock_hash.num_used = 0;
						slot->lwlock_hash.hash_size = hash_size;
						slot->lwlock_hash.max_entries = max_entries;

						/*
						 * Initialise the hash slot table to the empty
						 * sentinel.  The DSA region was zeroed above
						 * (DSA_ALLOC_ZERO), but the empty sentinel is
						 * 0xFFFF, not 0.
						 */
						slot_entries = (LWLockTimingHashEntry *)
							((char *) slot + sizeof(WaitEventTimingState));
						for (j = 0; j < hash_size; j++)
							slot_entries[j].tranche_id = LWLOCK_TIMING_EMPTY_SLOT;
					}

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
		{
			WaitEventTimingState *first;

			WaitEventTimingArray = (char *)
				dsa_get_address(timing_dsa,
								WaitEventTimingCtl->timing_array);

			/*
			 * Recover the dimensions from the first slot's lwlock_hash
			 * header.  All slots share the same dimensions, set at
			 * allocation time.  Cache the stride backend-locally so
			 * wet_slot() is a single multiply-and-add.
			 */
			first = (WaitEventTimingState *) WaitEventTimingArray;
			wait_event_timing_max_entries = first->lwlock_hash.max_entries;
			wait_event_timing_hash_size = first->lwlock_hash.hash_size;
			wait_event_timing_per_backend_stride =
				wait_event_timing_slot_size(wait_event_timing_max_entries);
		}
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
	WaitEventTimingState *slot;

	if (my_wait_event_timing != NULL)
		return;

	if (MyProc == NULL)
		return;

	/*
	 * Lazy attach allocates memory (via wait_event_timing_attach_array ->
	 * dsa_attach -> dsm_attach -> MemoryContextAlloc).  In a critical
	 * section, MemoryContextAlloc Assert-fails on
	 * "CritSectionCount == 0 || allowInCritSection".  A backend's very
	 * first wait event after wait_event_capture is enabled can land
	 * inside a critical section -- e.g. a parallel worker that hasn't
	 * yet emitted any wait events does so for the first time in
	 * BufferSetHintBits16 -> XLogSaveBufferForHint -> XLogInsert ->
	 * LWLockAcquire, with XLogInsert holding a critical section.
	 *
	 * Skipping the attach in that case silently drops the in-flight
	 * wait event but keeps the backend alive.  The very next wait
	 * event outside any critical section will hit this function again
	 * and attach successfully, after which the hot path no longer
	 * routes through here.  Wait events emitted inside critical
	 * sections are by their nature brief, infrequent (critical
	 * sections are short by design), and would be dropped anyway if
	 * the backend exited from a crash here -- so losing them at the
	 * very-first-attach moment is an acceptable tradeoff against the
	 * Assert-induced abort.
	 */
	if (CritSectionCount > 0)
		return;

	procNumber = GetNumberFromPGProc(MyProc);
	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	if (!wait_event_timing_attach_array(true))
		return;

	slot = wet_slot(procNumber);

	/*
	 * Clear this backend's slot the first time it is used after backend
	 * start.  The DSA-allocated region is zeroed on creation, but a later
	 * backend may inherit a slot previously occupied by an exited
	 * backend; explicit zero here keeps stats accurate across slot reuse.
	 * Matches the old per-backend init performed by
	 * pgstat_set_wait_event_timing_storage() in the eager-shmem design.
	 *
	 * Initialisation order: zero the slot through the local `slot` first,
	 * THEN publish the result to my_wait_event_timing.  This keeps the
	 * single-backend invariant clean: at no point in this backend can
	 * `my_wait_event_timing != NULL` coincide with `*my_wait_event_timing`
	 * being partially initialised.  The hot-path inline gate
	 *
	 *   if (unlikely(my_wait_event_timing == NULL))
	 *       pgstat_wait_event_timing_lazy_attach();
	 *   ... my_wait_event_timing->wait_start = ... ;
	 *
	 * relies on that ordering: a non-NULL pointer means the slot is
	 * ready for the very next store.
	 *
	 * Note that cross-backend readers do NOT go through
	 * my_wait_event_timing -- they index WaitEventTimingArray[procNumber]
	 * directly via pgstat_get_wait_event_timing(), guarded by
	 * pgstat_get_beentry_by_proc_number() which filters dead/recycled
	 * slots.  So this reordering is a same-backend tidiness fix; it does
	 * not address (and does not need to address) any cross-backend
	 * publication ordering, of which there is none.
	 */
	memset(slot->events, 0, sizeof(slot->events));
	lwlock_timing_hash_clear(slot);
	slot->reset_count = 0;
	slot->lwlock_overflow_count = 0;
	slot->flat_overflow_count = 0;
	slot->current_event = 0;
	INSTR_TIME_SET_ZERO(slot->wait_start);

	my_last_reset_generation = pg_atomic_read_u32(&slot->reset_generation);

	/* Publish only after the slot is fully initialised. */
	my_wait_event_timing = slot;
}

/*
 * Report the shared memory space needed for trace ring buffer control.
 * Only a small control struct is in fixed shmem; the actual ring buffers
 * are allocated lazily via DSA.  At ~24 bytes/slot, the slot array adds
 * ~26 KB at a default MaxBackends, negligible compared to the ring
 * memory itself.
 */
static Size
WaitEventTraceControlShmemSize(void)
{
	return add_size(offsetof(WaitEventTraceControl, trace_slots),
					mul_size(NUM_WAIT_EVENT_TIMING_SLOTS,
							 sizeof(WaitEventTraceSlot)));
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
	{
		WaitEventTraceSlot *s = &WaitEventTraceCtl->trace_slots[i];

		pg_atomic_init_u64(&s->generation, 0);
		pg_atomic_init_u32(&s->state, WAIT_EVENT_TRACE_SLOT_FREE);
		s->pad = 0;
		s->ring_ptr = InvalidDsaPointer;
	}
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
static void
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
 * Transition our trace ring slot to ORPHANED on backend exit.
 *
 * Registered as a before_shmem_exit callback.  Runs BEFORE
 * dsm_backend_shutdown() detaches the DSA.
 *
 * Crucially, we do NOT free the ring here.  The ring stays allocated in
 * DSA so that cross-backend consumers (ASH/AWR/10046-style monitoring
 * background workers, future or external) can read the dying backend's
 * final waits -- the original "free at exit" design lost data the
 * instant a worker terminated, which was particularly bad for parallel
 * workers exiting in milliseconds at end-of-parallel-query.  See the
 * lifecycle comment on WaitEventTraceControl for the full design
 * rationale and the bounded-memory cost we accept in exchange.
 *
 * The ORPHANED slot is reclaimed in one of two ways:
 *   (a) a new backend at this procNumber calls
 *       wait_event_trace_clear_orphan_at_init() at backend init, or
 *   (b) the DBA calls pg_stat_clear_orphaned_wait_event_rings().
 *
 * State transition order matters: bump generation BEFORE storing the
 * new state, so cross-backend readers that snapshot
 * (generation_before, state, ring_ptr, generation_after) under the
 * lock see a consistent (state, ring_ptr) pair iff generation didn't
 * change.  We hold the lock for the whole transition, but readers do
 * not have to (they just take it briefly to snapshot the ring
 * contents); the generation check is what makes the unlocked-read
 * path safe.
 */
static void
wait_event_trace_before_shmem_exit(int code, Datum arg)
{
	int		procNumber = DatumGetInt32(arg);
	WaitEventTraceSlot *slot;

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	slot = &WaitEventTraceCtl->trace_slots[procNumber];

	/*
	 * If this backend never ended up with an OWNED slot (e.g. capture
	 * was off the whole session, or the trace was released back to FREE
	 * via assign_wait_event_capture going trace -> off), there is
	 * nothing to transition.  Read state without the lock first as a
	 * fast-path check; the authoritative re-check happens under the
	 * lock below.
	 */
	if (pg_atomic_read_u32(&slot->state) != WAIT_EVENT_TRACE_SLOT_OWNED)
	{
		my_wait_event_trace = NULL;
		return;
	}

	LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);

	if (pg_atomic_read_u32(&slot->state) == WAIT_EVENT_TRACE_SLOT_OWNED &&
		DsaPointerIsValid(slot->ring_ptr))
	{
		/*
		 * Bump generation first so any reader that snapped the old
		 * generation will detect the change on its post-read recheck
		 * and discard its read.  Then publish the ORPHANED state.
		 * Keep ring_ptr valid -- the data is what we want to preserve.
		 */
		pg_atomic_fetch_add_u64(&slot->generation, 1);
		pg_atomic_write_u32(&slot->state, WAIT_EVENT_TRACE_SLOT_ORPHANED);
	}

	LWLockRelease(&WaitEventTraceCtl->lock);

	my_wait_event_trace = NULL;
}

/*
 * Allocate (or re-acquire) a trace ring buffer for this backend via DSA.
 * Called when wait_event_capture is set to 'trace'.
 *
 * Slot state at entry will be one of:
 *
 *   FREE     fresh slot (or one cleared on this backend's init by
 *            wait_event_trace_clear_orphan_at_init): allocate a new
 *            ring, transition slot to OWNED, bump generation.
 *
 *   OWNED    we already attached earlier in this same backend's life
 *            (e.g. user toggled capture trace->stats->trace; the
 *            stats step calls wait_event_trace_release_slot which
 *            transitions back to FREE, but our cached
 *            my_wait_event_trace was cleared on the way down -- so
 *            seeing OWNED here at attach time means a different
 *            backend somehow ended up with this procNumber, which
 *            cannot happen because procNumber is per-backend and a
 *            single backend can only run one attach at a time.  We
 *            still tolerate this state defensively by re-mapping the
 *            existing ring rather than leaking a second allocation.
 *
 *   ORPHANED can never be observed here: a new backend's
 *            pgstat_set_wait_event_timing_storage() called
 *            wait_event_trace_clear_orphan_at_init() before any
 *            wait-event capture path can run, so any prior orphan has
 *            already been demoted to FREE.  Treated as a safety check
 *            (Assert in debug builds).
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
	WaitEventTraceSlot *slot;
	dsa_pointer p;
	WaitEventTraceState *ts;
	uint32		state_now;

	if (in_attach)
		return;

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	/*
	 * Skip the attach if we are inside a critical section.  Below this
	 * point we call dsa_create / dsa_attach / dsa_allocate_extended,
	 * all of which can allocate memory via MemoryContextAlloc and
	 * Assert-fail on "CritSectionCount == 0 || allowInCritSection".
	 * The very-first wait event after wait_event_capture = trace can
	 * land inside a critical section (e.g. a parallel worker scanning
	 * a heap page hits BufferSetHintBits16 -> XLogSaveBufferForHint ->
	 * XLogInsert -> LWLockAcquire, with the XLogInsert critical
	 * section open).
	 *
	 * Skipping here silently drops the in-flight wait event (it is
	 * not traced) but keeps the backend alive.  The next wait event
	 * outside any critical section will hit this function again and
	 * attach successfully.  See the matching guard in
	 * pgstat_wait_event_timing_lazy_attach.
	 */
	if (CritSectionCount > 0)
		return;

	slot = &WaitEventTraceCtl->trace_slots[procNumber];

	in_attach = true;
	PG_TRY();
	{
		state_now = pg_atomic_read_u32(&slot->state);

		/*
		 * ORPHANED is normally impossible at attach time --
		 * pgstat_set_wait_event_timing_storage() at backend init calls
		 * wait_event_trace_clear_orphan_at_init() which demotes any
		 * inherited orphan to FREE.  But there is one case where this
		 * backend can legitimately observe its own slot in the
		 * ORPHANED state: after we have already run
		 * wait_event_trace_before_shmem_exit() (transitioning the slot
		 * to ORPHANED on exit), a later before_shmem_exit callback
		 * (e.g. pgstat_io_flush_cb during proc_exit shutdown) can
		 * contend on an LWLock that emits a wait event, which calls
		 * pgstat_report_wait_end_timing() -> wait_event_trace_attach()
		 * after my_wait_event_trace has been cleared.  We must not
		 * re-attach in that case: we are dying, the ring is now
		 * post-mortem data for cross-backend readers, and the writer
		 * invariant must hold.  Skip the trace for any wait events
		 * emitted after our own exit transition.
		 */
		if (state_now == WAIT_EVENT_TRACE_SLOT_ORPHANED)
		{
			/* PG_FINALLY below clears in_attach. */
		}
		else if (state_now == WAIT_EVENT_TRACE_SLOT_OWNED &&
				 DsaPointerIsValid(slot->ring_ptr))
		{
			/* Already have a ring buffer; re-map to it. */
			wait_event_trace_ensure_dsa();
			my_wait_event_trace = dsa_get_address(trace_dsa, slot->ring_ptr);
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
			/*
			 * Publish ring_ptr BEFORE transitioning state to OWNED.
			 * Cross-backend readers that observe state==OWNED outside
			 * the lock then see a valid ring_ptr.  Bump generation
			 * last so any reader that snapped the prior generation
			 * will detect the change.
			 */
			slot->ring_ptr = p;
			pg_atomic_write_u32(&slot->state, WAIT_EVENT_TRACE_SLOT_OWNED);
			pg_atomic_fetch_add_u64(&slot->generation, 1);
			LWLockRelease(&WaitEventTraceCtl->lock);

			my_wait_event_trace = ts;
			my_trace_proc_number = procNumber;

			/*
			 * Register cleanup to run BEFORE dsm_backend_shutdown()
			 * detaches the DSA.  The before_shmem_exit callbacks run in
			 * LIFO order before DSM detach, so the ORPHANED transition
			 * (which does not actually free the ring) is safe at that
			 * point.
			 *
			 * Guarded by shmem_exit_registered because under the
			 * release-on-disable policy (see wait_event_trace_release_slot
			 * and assign_wait_event_capture) the allocate branch can run
			 * multiple times per backend lifetime -- once per
			 * off/stats -> trace re-enable cycle.  The cleanup itself is
			 * idempotent (it short-circuits when state is not OWNED), so
			 * it is safe to invoke after a release-then-reattach cycle,
			 * but we still avoid growing the before_shmem_exit list.
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
static void
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
 * Important contrast with wait_event_trace_before_shmem_exit: backend
 * exit transitions the slot to ORPHANED (preserving data for
 * cross-backend readers); release_slot fully frees and returns to FREE
 * because the operator has explicitly disabled trace -- they have
 * affirmatively decided not to keep the data, so we honour that and
 * reclaim the memory immediately.  Subsequent re-enable allocates a
 * fresh ring via wait_event_trace_attach's allocate branch.
 *
 * The operation is LWLock-safe and does not raise -- dsa_free is pure
 * bookkeeping on the DSA freelist, no allocation and no ereport paths.
 * Safe to call from a GUC assign hook.
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
	WaitEventTraceSlot *slot;

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

	slot = &WaitEventTraceCtl->trace_slots[procNumber];

	in_release = true;
	PG_TRY();
	{
		LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);
		if (DsaPointerIsValid(slot->ring_ptr))
		{
			/*
			 * Bump generation first to invalidate any concurrent
			 * cross-backend snapshot, then free, then publish the FREE
			 * state with a NULL ring_ptr.  Order matters for unlocked
			 * readers that have already passed the state check.
			 */
			pg_atomic_fetch_add_u64(&slot->generation, 1);
			dsa_free(trace_dsa, slot->ring_ptr);
			slot->ring_ptr = InvalidDsaPointer;
			pg_atomic_write_u32(&slot->state, WAIT_EVENT_TRACE_SLOT_FREE);
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
 * Clear an orphaned trace ring at backend init time.
 *
 * Called from pgstat_set_wait_event_timing_storage() once the new
 * backend has its procNumber.  If the slot we're inheriting was left
 * ORPHANED by a previous backend (because we deliberately do not free
 * trace rings on backend exit -- see the lifecycle discussion on
 * WaitEventTraceControl), free the ring now so the new backend starts
 * with a clean FREE slot.  Subsequent wait_event_trace_attach() calls
 * (when this backend itself enables trace) will then take the
 * allocate branch.
 *
 * No-op when the slot is already FREE or OWNED: FREE means there's
 * nothing to clear; OWNED is impossible at backend init (only a
 * not-yet-exited backend can leave a slot OWNED, and procNumbers are
 * assigned exclusively).  We assert OWNED is not observed in debug
 * builds and conservatively skip the free in production.
 *
 * Robustness: this runs during InitProcess() (before the backend can
 * accept any work), and the work it performs -- dsa_attach() and
 * dsa_free() -- can raise ERROR on rare runtime failures (corrupted
 * DSA segment headers, descriptor exhaustion, mmap ENOMEM, etc.).
 * An uncaught ERROR here would propagate out of InitProcess() and
 * abort backend startup entirely, even for sessions that never
 * intended to use wait_event_capture.  To prevent the trace
 * feature's housekeeping from gating connection establishment, the
 * body is wrapped in PG_TRY()/PG_CATCH(): any error from dsa_attach
 * or dsa_free is captured, downgraded to a WARNING with a hint
 * pointing at the admin sweep function, and execution continues.
 * The orphan stays in place; it can be reclaimed by the next
 * backend that inherits the same procNumber (if the underlying
 * problem was transient), by pg_stat_clear_orphaned_wait_event_rings(),
 * or at next cluster restart.
 */
static void
wait_event_trace_clear_orphan_at_init(int procNumber)
{
	WaitEventTraceSlot *slot;
	uint32		state_now;
	MemoryContext caller_cxt;

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	slot = &WaitEventTraceCtl->trace_slots[procNumber];

	state_now = pg_atomic_read_u32(&slot->state);
	if (state_now != WAIT_EVENT_TRACE_SLOT_ORPHANED)
	{
		Assert(state_now != WAIT_EVENT_TRACE_SLOT_OWNED);
		return;
	}

	/*
	 * Save CurrentMemoryContext so the PG_CATCH path can copy the
	 * error data into a context that survives FlushErrorState().
	 * FlushErrorState() calls MemoryContextReset(ErrorContext), so
	 * CopyErrorData() must run in a different context or the
	 * returned ErrorData becomes a dangling pointer.
	 */
	caller_cxt = CurrentMemoryContext;

	PG_TRY();
	{
		/*
		 * The trace DSA is shared across the cluster.  We must attach
		 * to it before calling dsa_free (which needs the dsa_area
		 * pointer).  The DSA was created by some earlier backend that
		 * wrote a trace record (otherwise the slot couldn't have
		 * ended up ORPHANED), so the handle in WaitEventTraceCtl is
		 * valid; ensure_dsa() will attach.  Both ensure_dsa() and
		 * dsa_free() can raise ERROR; the PG_CATCH below downgrades
		 * any such error to a WARNING so backend startup is not
		 * blocked.
		 */
		wait_event_trace_ensure_dsa();

		LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u32(&slot->state) == WAIT_EVENT_TRACE_SLOT_ORPHANED &&
			DsaPointerIsValid(slot->ring_ptr))
		{
			pg_atomic_fetch_add_u64(&slot->generation, 1);
			dsa_free(trace_dsa, slot->ring_ptr);
			slot->ring_ptr = InvalidDsaPointer;
			pg_atomic_write_u32(&slot->state, WAIT_EVENT_TRACE_SLOT_FREE);
		}
		LWLockRelease(&WaitEventTraceCtl->lock);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/*
		 * Release any LWLocks we (or anything we called) might
		 * still hold.  Two paths can leave WaitEventTraceCtl->lock
		 * held when control reaches here:
		 *
		 *   1. The outer LWLockAcquire above succeeded and dsa_free
		 *      raised before we reached LWLockRelease.
		 *   2. wait_event_trace_ensure_dsa() raised inside its own
		 *      LWLockAcquire/dsa_attach/LWLockRelease region.
		 *
		 * We are running during InitProcess(), BEFORE any
		 * transaction or PostgresMain sigsetjmp has been set up,
		 * so PG's standard "AbortTransaction -> LWLockReleaseAll"
		 * cleanup does NOT fire on the longjmp into PG_CATCH.
		 * Without an explicit release here the lock would stay
		 * held for the lifetime of this backend, blocking every
		 * future LW_EXCLUSIVE acquirer (the orphan-clear sweep,
		 * release_slot, before_shmem_exit transitions, and
		 * subsequent backends' clear_orphan_at_init).  That would
		 * be strictly worse than the original failure-startup
		 * behavior this commit set out to fix.
		 *
		 * LWLockReleaseAll() is the idiomatic catch-path lock
		 * cleanup used by the standalone aux-process error
		 * handlers (walwriter.c, checkpointer.c, pgarch.c).  It
		 * is safe to call broadly here because pgstat_set_wait_
		 * event_timing_storage runs at a fixed point in
		 * InitProcess where the caller frame holds no other
		 * LWLocks across our return: the earlier InitProcess
		 * steps that touch LWLocks (ProcArrayAdd, etc.) release
		 * them before returning, and the subsequent steps that
		 * acquire LWLocks have not yet run.
		 */
		LWLockReleaseAll();

		/*
		 * Switch BACK to the caller's context before CopyErrorData
		 * so that edata is allocated in a context that survives
		 * FlushErrorState().  FlushErrorState() calls
		 * MemoryContextReset(ErrorContext); allocating edata in
		 * ErrorContext (the default at PG_CATCH entry on the error
		 * path) would make it a dangling pointer the moment we
		 * flush.  See the matching pattern in spi.c PG_CATCH
		 * branches.
		 */
		MemoryContextSwitchTo(caller_cxt);
		edata = CopyErrorData();
		FlushErrorState();

		ereport(WARNING,
				(errcode(edata->sqlerrcode),
				 errmsg("could not clear orphaned wait-event trace ring "
						"at backend init: %s", edata->message),
				 errdetail("Backend startup proceeds with the orphan "
						   "still allocated for procnumber %d.",
						   procNumber),
				 errhint("Run pg_stat_clear_orphaned_wait_event_rings() "
						 "to release the orphan when the underlying "
						 "condition is resolved.")));

		FreeErrorData(edata);
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
 *    large connection pools that briefly enable trace.  Freeing here
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

	/*
	 * If the previous occupant of this procNumber slot was a tracing
	 * backend that exited, its trace ring is still allocated in DSA in
	 * ORPHANED state (see wait_event_trace_before_shmem_exit and the
	 * lifecycle discussion on WaitEventTraceControl).  Free it now so
	 * this backend starts with a clean FREE slot; otherwise the next
	 * wait_event_trace_attach call would observe OWNED-but-not-our-data
	 * (impossible by invariant) or, with the eventual addition of
	 * post-mortem cross-backend reads, a freshly attached writer would
	 * end up appending to a previous backend's records.
	 */
	wait_event_trace_clear_orphan_at_init(procNumber);
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
		lwlock_timing_hash_clear(my_wait_event_timing);
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
				entry = lwlock_timing_lookup(my_wait_event_timing,
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
						 errhint("This backend uses more than %d distinct LWLock tranches; raise wait_event_timing_max_tranches.",
								 wait_event_timing_max_entries)));
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

				/*
				 * Injection point used by the regression test for the
				 * position-encoded identity seqlock in
				 * emit_wait_event_trace_for_procnumber().  Stalling here
				 * widens the window between the write_pos store and the
				 * rec->seq store, simulating the weak-memory visibility
				 * order that would otherwise be unreachable on x86.  A
				 * cross-backend reader observing the new write_pos
				 * while the rec->seq update has not yet happened MUST
				 * skip this slot via the identity check; without the
				 * identity check the reader would emit a stale record
				 * from the previous ring cycle with the wrong ring
				 * index.  Compiled out unless --enable-injection-points
				 * is set.
				 */
				INJECTION_POINT("wait-event-trace-after-write-pos", NULL);

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
		WaitEventTimingState *state = wet_slot(backend_idx);
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
				values[2] = Int32GetDatum(backend_idx);
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
		{
			LWLockTimingHashEntry *entries = wet_lwlock_hash_entries(state);
			WaitEventTimingEntry *events = wet_lwlock_hash_events(state);
			int			hash_size = state->lwlock_hash.hash_size;

		for (i = 0; i < hash_size; i++)
		{
			LWLockTimingHashEntry *he = &entries[i];
			WaitEventTimingEntry *entry;
			Datum		values[10];
			bool		nulls[10];
			uint32		wait_event_info;
			const char *event_type;
			const char *event_name;
			int			bucket;

			if (he->tranche_id == LWLOCK_TIMING_EMPTY_SLOT)
				continue;

			entry = &events[he->dense_idx];
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
			values[2] = Int32GetDatum(backend_idx);
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
 * WAIT_EVENT_TRACE_RING_SIZE (131072) records; full materialization
 * caps the per-call cost at ~4 MB of tuplestore memory, which is
 * acceptable for the use case this SRF is designed for: interactive
 * own-session diagnostics from psql.
 *
 * This SRF is NOT the path for cross-backend monitoring tools --
 * cross-backend readers (ASH/AWR/10046-style background workers that
 * consume the per-backend trace rings) should NOT call this function
 * via SPI.
 * It is hard-coded to return only the calling backend's own ring,
 * so a bgworker calling SELECT * FROM pg_backend_wait_event_trace
 * would get only the bgworker's own (typically empty) ring, not the
 * target backend's data.
 *
 * Cross-backend consumers must instead use the lock + DSA-snapshot
 * pattern documented on WaitEventTraceControl in wait_event_timing.h:
 * acquire WaitEventTraceCtl->lock in LW_SHARED, resolve trace_ptrs[
 * procNumber] via dsa_get_address, snapshot the records of interest
 * into local memory, release the lock, then process the snapshot.
 * That path bypasses this SRF entirely and is the supported
 * cross-backend interface for monitoring extensions and bgworkers.
 *
 * value-per-call (deferred) SRF mode would let an interactive
 * "SELECT ... FROM pg_backend_wait_event_trace LIMIT N" short-circuit
 * the materialisation, but converting this function would require
 * spanning the wait_event_trace_srf_in_progress flag (and its
 * deferred-free coordination with assign_wait_event_capture; see
 * issue #8) across multiple SRF callbacks plus a transaction-cleanup
 * registration to handle LIMIT abandonment.  The complexity is not
 * justified for the diagnostic use case, especially since cross-
 * backend monitoring (the consumer that would actually benefit from
 * streaming) goes through the snapshot pattern above instead.
 * Interactive callers who want only recent records should use
 * "ORDER BY seq DESC LIMIT N" -- the LIMIT is applied after
 * materialisation but the cost stays bounded by the ring size.
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
 * One element of the local result buffer.  Pairs a per-record copy
 * with the original ring index (used as the seq output column).
 */
typedef struct WetValidRecord
{
	uint64		ring_index;		/* original index in the writer's ring */
	WaitEventTraceRecord rec;
} WetValidRecord;

/*
 * Snapshot the trace ring for a given procNumber and emit records into
 * the SRF's tuplestore.  Returns silently for FREE slots, out-of-range
 * procnumbers, slots whose ring was never allocated, and slots whose
 * write_pos is zero.
 *
 * Cross-backend reader protocol implemented here:
 *
 *   1. Read slot->state without the lock as a cheap "worth visiting"
 *      check; FREE -> nothing to emit.
 *   2. Allocate the worst-case result buffer BEFORE taking the lock,
 *      so the palloc -- which can bottom out in a glibc mmap syscall
 *      for the ~5 MB worst-case size -- runs without holding the
 *      WaitEventTraceCtl lock.
 *   3. Acquire WaitEventTraceCtl->lock in LW_SHARED.  All slot
 *      transitions take LW_EXCLUSIVE, so the slot's identity, state,
 *      and ring_ptr are stable for the duration of the iteration.
 *   4. Re-check state under the lock and resolve ring_ptr via
 *      dsa_get_address.  Read write_pos.
 *   5. Iterate every live ring index [read_start, write_pos).  For
 *      each record do the per-record POSITION-ENCODED IDENTITY
 *      seqlock check ON SHARED MEMORY (see the comment on the loop
 *      below).
 *   6. Release the lock.
 *   7. Walk the local result array and emit rows into the tuplestore.
 *      This is the expensive part (potential disk spill); doing it
 *      after release minimises lock-hold time.
 *
 * Why per-record seqlock against shared memory, not against a local
 * memcpy of the full ring: the protocol requires the two seq reads
 * to go to the SAME shared-memory location at DIFFERENT TIMES, with
 * the payload read between them.  A bulk memcpy then seqlock-on-
 * local-copy reads the same frozen byte twice, the check degenerates
 * to a no-op, and torn / stale-cycle reads slip through.
 *
 * Why position-encoded identity, not just parity: the writer encodes
 * the ring position into the seq value (mid-write = pos*2+1, complete
 * = pos*2+2).  After RING_SIZE writes the slot wraps and is rewritten
 * with a new numerically-distinct seq.  A parity-only check accepts
 * any stable even seq -- including the PREVIOUS cycle's seq if cross-
 * process visibility puts the new write_pos ahead of the new seq
 * update.  See the loop body for the four failure modes the identity
 * check rejects.
 *
 * Holding LW_SHARED throughout the iteration also makes the
 * generation-counter retry unnecessary for this caller: slot
 * transitions take LW_EXCLUSIVE and therefore cannot happen while we
 * hold LW_SHARED.  The generation counter is still part of the
 * cross-backend reader contract on WaitEventTraceControl for external
 * readers that follow a different lock-release pattern (e.g. an
 * extension that wants to release the lock between batches of records
 * and re-acquire), but this in-tree implementation does not release
 * the lock mid-iteration.
 *
 * Both OWNED and ORPHANED slots are read uniformly.  For OWNED the
 * live owner is concurrently writing; the seqlock catches torn reads.
 * For ORPHANED the records are immutable post-mortem so the check is
 * essentially a pass-through (it still correctly skips at most one
 * trailing odd-seq record if the owner died mid-write).
 *
 * Lock-hold is O(write_pos - read_start) shared-memory loads, at
 * roughly the same wall-clock cost as a single 4 MB memcpy of the
 * full ring (~1 ms on modern hardware), with no I/O and no syscalls.
 */
static void
emit_wait_event_trace_for_procnumber(int procNumber, ReturnSetInfo *rsinfo)
{
	WaitEventTraceSlot *slot;
	WaitEventTraceState *ts;
	WetValidRecord *valid_records = NULL;
	uint64		valid_count = 0;
	uint64		write_pos;
	uint64		read_start;
	uint64		i;
	uint32		state_now;

	if (WaitEventTraceCtl == NULL)
		return;

	/*
	 * Range check.  Negative or out-of-range procnumbers return an
	 * empty result rather than ERRORing because the most natural use
	 * pattern for cross-backend readers is to iterate every possible
	 * slot index (a monitoring background worker doesn't know the
	 * exact NUM_WAIT_EVENT_TIMING_SLOTS at SQL level), and silent-
	 * empty for out-of-range matches the behaviour of sister functions
	 * like pg_stat_get_wait_event_timing(NULL) which iterate the
	 * shared array internally.  FREE-but-in-range slots also return
	 * empty (see the state check below); the caller cannot
	 * distinguish out-of-range from FREE, which is fine.
	 */
	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
		return;

	slot = &WaitEventTraceCtl->trace_slots[procNumber];

	/*
	 * If the trace DSA was never created (no backend in the cluster
	 * has ever set wait_event_capture = trace), every slot is still
	 * in its initial FREE state.  Skip without taking the lock.
	 */
	if (WaitEventTraceCtl->trace_dsa_handle == DSA_HANDLE_INVALID)
		return;

	/* Unlocked fast-path check; the authoritative check is under the
	 * lock below. */
	if (pg_atomic_read_u32(&slot->state) == WAIT_EVENT_TRACE_SLOT_FREE)
		return;

	wait_event_trace_ensure_dsa();
	if (trace_dsa == NULL)
		return;

	/*
	 * Allocate the worst-case result buffer BEFORE taking the lock.
	 * The buffer is sized for the full ring (~5 MB at default
	 * RING_SIZE=128K); on a near-empty ring most of it goes unused,
	 * but that is preferable to holding the WaitEventTraceCtl lock
	 * during a palloc that may bottom out in a glibc mmap() syscall
	 * (allocations above the malloc-mmap threshold).  Glibc's
	 * arena-internal mutex around the syscall would serialise every
	 * concurrent reader of this lock through one VMA-modifying
	 * kernel operation; sizing the alloc outside the lock keeps the
	 * lock-hold time bounded by the per-record loop alone.
	 *
	 * After we acquire the lock we will either consume this buffer
	 * (writing up to (write_pos - read_start) entries) or release
	 * it unused on an early return.
	 */
	valid_records = palloc(sizeof(WetValidRecord) *
						   WAIT_EVENT_TRACE_RING_SIZE);

	LWLockAcquire(&WaitEventTraceCtl->lock, LW_SHARED);

	state_now = pg_atomic_read_u32(&slot->state);
	if (state_now == WAIT_EVENT_TRACE_SLOT_FREE ||
		!DsaPointerIsValid(slot->ring_ptr))
	{
		LWLockRelease(&WaitEventTraceCtl->lock);
		pfree(valid_records);
		return;
	}

	ts = (WaitEventTraceState *) dsa_get_address(trace_dsa, slot->ring_ptr);
	write_pos = pg_atomic_read_u64(&ts->write_pos);

	if (write_pos == 0)
	{
		LWLockRelease(&WaitEventTraceCtl->lock);
		pfree(valid_records);
		return;
	}

	/* Live range: oldest available to newest. */
	read_start = (write_pos > WAIT_EVENT_TRACE_RING_SIZE)
		? write_pos - WAIT_EVENT_TRACE_RING_SIZE : 0;

	for (i = read_start; i < write_pos; i++)
	{
		WaitEventTraceRecord *rec_shared =
			&ts->records[i & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		WetValidRecord *out = &valid_records[valid_count];
		uint32		expected_seq;
		uint32		seq_before;
		uint32		seq_after;

		/*
		 * Position-encoded seqlock identity check (NOT just parity).
		 *
		 * The writer encodes the ring position into the seq value:
		 * mid-write -> (uint32)(pos * 2 + 1), complete -> + 2.  After
		 * RING_SIZE writes the slot wraps and the same memory location
		 * gets a new seq value (next_pos * 2 + 2) that is numerically
		 * distinct from the previous cycle's seq.
		 *
		 * A parity-only check (skip on odd seq, accept on stable even)
		 * is INSUFFICIENT for this layout in the cross-backend case:
		 * if the writer just incremented write_pos to pos+1 but
		 * cross-process cache coherence has not yet propagated the
		 * subsequent rec->seq = (pos*2+1) store, this reader at
		 * i = pos would see the previous cycle's complete-even seq
		 * (from logical position pos - RING_SIZE).  Both seq_before
		 * and seq_after would read that stale even value, parity
		 * passes, identity-against-itself passes, and a record
		 * belonging to the PREVIOUS cycle gets emitted with the new
		 * ring_index = pos.  Silent data corruption (wrong attribution,
		 * not torn bytes).
		 *
		 * The fix is identity against EXPECTED: a record is valid for
		 * iterator position i if and only if its seq equals
		 * (uint32)(i * 2 + 2) -- the writer's encoded "complete" value
		 * for that exact ring position.  This rejects:
		 *
		 *   * Stale prior cycle (seq <  expected): writer hasn't yet
		 *     advanced rec->seq for the current cycle.
		 *   * Mid-write current cycle (seq == expected - 1, odd):
		 *     writer is in the payload write window.
		 *   * Ring wrapped past us (seq >  expected): the writer
		 *     completed a later cycle on this slot during our read.
		 *
		 * The uint32 wraparound at 2^31 cycles is safe: we use exact
		 * equality, and the writer's existing wrap-safety argument
		 * (sizeof(seq) > worst-case in-flight window by 11 orders of
		 * magnitude) covers the seq value.
		 */
		expected_seq = (uint32)(i * 2 + 2);

		seq_before = rec_shared->seq;
		pg_read_barrier();

		if (seq_before != expected_seq)
			continue;

		out->rec = *rec_shared;		/* one 32-byte structure copy */

		pg_read_barrier();
		seq_after = rec_shared->seq;

		if (seq_after != expected_seq)
			continue;

		out->ring_index = i;
		valid_count++;
	}

	LWLockRelease(&WaitEventTraceCtl->lock);

	/*
	 * Walk the local result array and emit rows.  No shared-memory
	 * access from here on, so spills to disk by the tuplestore (if
	 * the result is large) do not hold any wait-event-timing lock.
	 */
	for (i = 0; i < valid_count; i++)
	{
		WetValidRecord *vr = &valid_records[i];
		WaitEventTraceRecord *rec = &vr->rec;
		Datum		values[6];
		bool		nulls[6];
		const char *event_type;
		const char *event_name;
		uint8		rtype = rec->record_type;
		uint32		event_info;
		int64		duration_ns;
		int64		query_id;

		if (rtype == TRACE_WAIT_EVENT)
		{
			event_info = rec->data.wait.event;
			duration_ns = rec->data.wait.duration_ns;
			query_id = 0;

			/* Skip empty wait events. */
			if (event_info == 0)
				continue;

			event_type = pgstat_get_wait_event_type(event_info);
			event_name = pgstat_get_wait_event(event_info);
		}
		else if (rtype == TRACE_QUERY_START)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
			event_type = "Query";
			event_name = "QueryStart";
		}
		else if (rtype == TRACE_QUERY_END)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
			event_type = "Query";
			event_name = "QueryEnd";
		}
		else if (rtype == TRACE_EXEC_START)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
			event_type = "Query";
			event_name = "ExecStart";
		}
		else if (rtype == TRACE_EXEC_END)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
			event_type = "Query";
			event_name = "ExecEnd";
		}
		else
		{
			/* Unrecognised record_type -- skip defensively. */
			continue;
		}

		if (event_type == NULL || event_name == NULL)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum((int64) vr->ring_index);
		values[1] = Int64GetDatum(rec->timestamp_ns);
		values[2] = CStringGetTextDatum(event_type);
		values[3] = CStringGetTextDatum(event_name);
		values[4] = Float8GetDatum((double) duration_ns / 1000.0);
		values[5] = Int64GetDatum(query_id);

		tuplestore_putvalues(rsinfo->setResult,
							 rsinfo->setDesc,
							 values, nulls);
	}

	pfree(valid_records);
}

/*
 * SQL function: pg_get_wait_event_trace(procnumber int4)
 *
 * Cross-backend trace ring reader.  Returns the records from the trace
 * ring belonging to the backend that currently or previously occupied
 * the given procNumber slot.  Reads OWNED and ORPHANED slots uniformly;
 * FREE slots return an empty result.
 *
 * This SRF is the in-tree consumer of the orphan-preserved trace data:
 * a backend that exited while wait_event_capture = trace leaves its
 * ring allocated in DSA in ORPHANED state, and this function reads it
 * until either a new backend takes over the same procNumber or the
 * DBA calls pg_stat_clear_orphaned_wait_event_rings().  External
 * monitoring background workers (ASH/AWR/10046-style readers) follow
 * the same snapshot pattern documented on WaitEventTraceControl in
 * wait_event_timing.h; this function serves as both the reference
 * implementation and a DBA-facing diagnostic tool.
 *
 * Privileges: REVOKE'd from PUBLIC and GRANT'ed to pg_read_all_stats
 * in system_views.sql, matching the privilege model of the session-
 * local view pg_backend_wait_event_trace.
 *
 * The procnumber argument can be obtained from the procnumber column
 * of pg_stat_get_wait_event_timing or pg_stat_get_wait_event_timing_
 * overflow.  For pid-keyed access against live backends, callers can
 * do:
 *
 *   SELECT * FROM pg_get_wait_event_trace(
 *       (SELECT procnumber FROM pg_stat_get_wait_event_timing(<pid>)
 *        WHERE pid = <pid> LIMIT 1));
 *
 * Note that pid-keyed access cannot read ORPHANED slots because a
 * dying backend's pid is removed from procArray on exit; for
 * post-mortem reading of short-lived backends (parallel workers,
 * autovacuum, walsender) the procNumber must be captured before the
 * backend exits, or discovered by iterating procnumbers in a
 * monitoring background worker.
 */
Datum
pg_get_wait_event_trace(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int32		procNumber = PG_GETARG_INT32(0);

	InitMaterializedSRF(fcinfo, 0);

	emit_wait_event_trace_for_procnumber((int) procNumber, rsinfo);

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

	pg_atomic_fetch_add_u32(&wet_slot(slot_idx)->reset_generation, 1);

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
 *       (capped by wait_event_timing_max_tranches) was full.
 *   flat_overflow_count:   number of non-LWLock wait events that
 *       resolved to an unknown / out-of-range class index and therefore
 *       could not be mapped to a histogram slot.
 *   reset_count:           number of resets this backend has *observed
 *       and acted on*, NOT a request counter.  Own-backend resets are
 *       synchronous and bump this once per call.  Cross-backend resets
 *       coalesce: if multiple pg_stat_reset_wait_event_timing(target)
 *       calls land between two of the target's wait_ends, the target
 *       observes them as a single reset and reset_count increments
 *       only once.  Callers polling for asynchronous-reset
 *       acknowledgment should watch for any increment (N -> N+1).
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
		WaitEventTimingState *state = wet_slot(backend_idx);
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
		values[2] = Int32GetDatum(backend_idx);
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
			lwlock_timing_hash_clear(my_wait_event_timing);
			my_wait_event_timing->reset_count++;
			my_wait_event_timing->lwlock_overflow_count = 0;
			my_wait_event_timing->flat_overflow_count = 0;
			my_wait_event_timing->current_event = 0;
		}
		PG_RETURN_VOID();
	}

	/*
	 * Cross-backend reset requires pg_signal_backend membership, matching
	 * the privilege model of pg_stat_reset_backend_stats(int4 pid) (the
	 * closest existing per-backend reset in the wider stats family).
	 *
	 * Why pg_signal_backend rather than naked superuser():
	 *
	 * 1) Operational alignment.  The role pg_signal_backend exists
	 *    specifically for "the operator who acts on other backends'
	 *    state" -- it gates pg_terminate_backend, pg_cancel_backend,
	 *    and pg_stat_reset_backend_stats already.  Resetting another
	 *    backend's wait-event timing is structurally the same kind of
	 *    operation (per-PID, addressable, bounded blast radius), so it
	 *    belongs to the same role.  Demanding superuser would create a
	 *    surplus-privilege gap: a DBA who can already TERMINATE the
	 *    target backend (strictly more invasive than resetting its
	 *    counters) would need to escalate to superuser just to wipe
	 *    its stats, which is operationally backwards.
	 *
	 * 2) Cluster-wide reset is a different decision.  See
	 *    pg_stat_reset_wait_event_timing_all() below, which keeps the
	 *    stricter superuser() gate -- different blast radius, different
	 *    role.  This split (per-backend = pg_signal_backend, cluster-wide
	 *    = superuser) reflects the principle that the role required for
	 *    an operation should match what the operation can affect.  The
	 *    fact that pg_stat_reset() (cluster-wide) actually only requires
	 *    pg_read_all_stats today is an inconsistency in PG's existing
	 *    surface; we deliberately do not extend that inconsistency here.
	 *
	 * 3) Information-disclosure concern is bounded.  The only
	 *    "destructive" property of a stats reset is that it erases
	 *    forensic evidence of past wait events.  Anyone with
	 *    pg_signal_backend can already terminate the target backend --
	 *    which terminates that forensic record by destroying the
	 *    backend itself.  A counter wipe is strictly less invasive.
	 */
	if (!has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to reset another backend's wait event timing"),
				 errdetail("Only roles with privileges of the \"pg_signal_backend\" role may reset another backend's wait event timing.")));

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
 *
 * Privilege model rationale (intentional asymmetry with the per-backend
 * variant pg_stat_reset_wait_event_timing(pid)):
 *
 *   * Per-backend reset uses pg_signal_backend, matching
 *     pg_stat_reset_backend_stats(pid).  The blast radius is one PID;
 *     anyone who can pg_terminate_backend the target can already
 *     destroy more forensic state than a counter wipe would.
 *
 *   * Cluster-wide reset is gated tighter because the blast radius is
 *     every backend in the cluster.  An operator with pg_signal_backend
 *     can disrupt one PID at a time (and must specify which); the
 *     cluster-wide reset wipes ALL backends' historical counters in a
 *     single call, which is meaningfully different in two ways:
 *
 *       (a) it can hide cross-tenant patterns that a forensic audit
 *           would have wanted to compare across backends, and
 *
 *       (b) it removes the per-call addressability that makes the
 *           per-backend variant auditable -- a log entry showing "user
 *           X reset PID Y" is more actionable than "user X wiped
 *           everything."
 *
 *     Requiring superuser for the cluster-wide variant matches the
 *     general PG principle that scope of authority should match scope
 *     of effect.  We deliberately do NOT mirror pg_stat_reset(), which
 *     today is gated only on pg_read_all_stats despite being similarly
 *     cluster-wide -- that's a pre-existing inconsistency in the wider
 *     stats family and not one we want to extend.
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

/*
 * SQL function: pg_stat_clear_orphaned_wait_event_rings()
 *
 * Free every trace ring whose owner has exited (slot state ORPHANED).
 * Returns the number of rings released.
 *
 * Why this exists.  When a backend that had wait_event_capture = trace
 * exits, we deliberately do NOT free its ~4 MB trace ring (see the
 * lifecycle discussion on WaitEventTraceControl): the data must remain
 * readable by cross-backend consumers (ASH/AWR/10046-style monitoring
 * background workers), and an exit-time dsa_free would defeat that.
 * The reclaim instead happens lazily in two places:
 *
 *   (a) wait_event_trace_clear_orphan_at_init(): when a new backend
 *       inherits the same procNumber slot at init, it frees the prior
 *       orphan as part of starting clean.  This handles the common
 *       case (busy clusters with connection churn) automatically.
 *
 *   (b) THIS FUNCTION: an explicit DBA-driven sweep that releases
 *       every currently orphaned ring at once.
 *
 * The pathological case (a) does not handle is "capture briefly
 * enabled, then disabled, on a cluster with long-lived pooled
 * connections that never exit".  In that scenario procNumbers do not
 * recycle, so prior orphans persist until cluster restart unless the
 * DBA calls this function.  Worst-case bound is
 * NUM_WAIT_EVENT_TIMING_SLOTS * sizeof(WaitEventTraceState) which is
 * ~400 MB at MaxBackends=100, ~4 GB at MaxBackends=1000 -- bounded
 * but worth a kill switch.
 *
 * Permissions: superuser-only, matching the cluster-wide reset
 * (pg_stat_reset_wait_event_timing_all).  This is a
 * cluster-scope memory-reclamation operation: it can disrupt any
 * concurrent cross-backend reader on any orphaned slot.  The
 * disruption is bounded (readers retry via the generation counter
 * and at worst skip one read) but the operation is still
 * cluster-wide, so the privilege model matches the reset variant
 * with the same blast radius.
 *
 * The function is safe to call even when no orphans exist (returns
 * 0) and even when capture is currently OFF (the slot array exists
 * unconditionally; only the rings are lazy).
 */
Datum
pg_stat_clear_orphaned_wait_event_rings(PG_FUNCTION_ARGS)
{
	int64		freed = 0;
	int			i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to clear orphaned wait event "
						"trace rings")));

	if (WaitEventTraceCtl == NULL)
		PG_RETURN_INT64(0);

	/*
	 * If no backend has ever enabled trace, the trace DSA was never
	 * created and there cannot be any ORPHANED slots: every slot is
	 * still in its initial FREE state.  Nothing to do.
	 */
	if (WaitEventTraceCtl->trace_dsa_handle == DSA_HANDLE_INVALID)
		PG_RETURN_INT64(0);

	/* Attach to the trace DSA so dsa_free() can be called. */
	wait_event_trace_ensure_dsa();
	if (trace_dsa == NULL)
		PG_RETURN_INT64(0);

	/*
	 * Walk every slot, taking and releasing WaitEventTraceCtl->lock per
	 * slot rather than holding it across the entire sweep.
	 *
	 * Rationale: at MaxBackends = 1000 with a fully-orphaned cluster
	 * the per-slot work (atomic state read + dsa_free + ring_ptr
	 * clear + atomic state write) totals a few microseconds; holding
	 * the lock across all slots would yield a millisecond-scale
	 * lock-hold window during which every concurrent backend startup
	 * (the lazy wait_event_trace_clear_orphan_at_init path), every
	 * cross-backend reader (pg_get_wait_event_trace and the external
	 * snapshot pattern), and every capture step-down or restore
	 * would stall.  PG's general convention is to keep LWLock-held
	 * windows in paths that compete with regular activity well under
	 * 100 microseconds; per-slot release/reacquire gives us a worst-
	 * case lock-hold of one slot's worth of work regardless of how
	 * many orphans exist cluster-wide.
	 *
	 * An unlocked fast-path read of slot->state skips non-ORPHANED
	 * slots without an LWLockAcquire/Release pair.  This is safe: if
	 * a slot races from non-ORPHANED to ORPHANED after we read it,
	 * we miss that orphan -- but the function is documented as a
	 * snapshot sweep, the missed orphan can be cleared by a
	 * subsequent call, and the same race exists for orphans that
	 * appear after the loop ends.  The authoritative re-check under
	 * the lock prevents racing on the dsa_free direction (we never
	 * free a slot whose owner became OWNED again).
	 *
	 * CHECK_FOR_INTERRUPTS at the top of the loop body lets the
	 * caller cancel a long sweep; with the previous single-lock
	 * structure the InterruptHoldoffCount elevation from
	 * LWLockAcquire deferred all cancellation until release.
	 */
	for (i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
	{
		WaitEventTraceSlot *slot = &WaitEventTraceCtl->trace_slots[i];

		CHECK_FOR_INTERRUPTS();

		/* Unlocked fast-path: skip non-ORPHANED slots cheaply. */
		if (pg_atomic_read_u32(&slot->state) != WAIT_EVENT_TRACE_SLOT_ORPHANED)
			continue;

		LWLockAcquire(&WaitEventTraceCtl->lock, LW_EXCLUSIVE);

		/*
		 * Authoritative re-check under the lock.  A concurrent
		 * clear_orphan_at_init may have already freed this slot.
		 */
		if (pg_atomic_read_u32(&slot->state) == WAIT_EVENT_TRACE_SLOT_ORPHANED &&
			DsaPointerIsValid(slot->ring_ptr))
		{
			pg_atomic_fetch_add_u64(&slot->generation, 1);
			dsa_free(trace_dsa, slot->ring_ptr);
			slot->ring_ptr = InvalidDsaPointer;
			pg_atomic_write_u32(&slot->state, WAIT_EVENT_TRACE_SLOT_FREE);
			freed++;
		}

		LWLockRelease(&WaitEventTraceCtl->lock);
	}

	PG_RETURN_INT64(freed);
}

#endif							/* USE_WAIT_EVENT_TIMING */
