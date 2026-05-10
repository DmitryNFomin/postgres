/*-------------------------------------------------------------------------
 *
 * wait_event_timing.h
 *	  Per-backend wait event timing and histogram infrastructure.
 *
 * When enabled via the wait_event_timing GUC, every call to
 * pgstat_report_wait_start()/pgstat_report_wait_end() records the wait
 * duration and accumulates per-event statistics (count, total time,
 * histogram) in shared memory.
 *
 * The overhead is two clock_gettime(CLOCK_MONOTONIC) calls per wait event
 * transition (~40-100 ns via VDSO), plus a few memory writes to per-backend
 * arrays.  No locking is needed because each backend writes only to its own
 * stats slot.
 *
 * Statistics are exposed via the pg_stat_wait_event_timing view
 * and pg_stat_get_wait_event_timing() SQL function.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/include/utils/wait_event_timing.h
 *-------------------------------------------------------------------------
 */
#ifndef WAIT_EVENT_TIMING_H
#define WAIT_EVENT_TIMING_H

#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "portability/instr_time.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/wait_event_types.h"

/*
 * Number of log2 histogram buckets.  Bin edges are powers of two on the
 * nanosecond axis: bucket i covers [2^(i+9), 2^(i+10)) ns, except bucket
 * 0 which covers [0, 1024) ns and the last bucket which covers
 * [2^(NBUCKETS+8), infinity) ns.  These boundaries approximate the
 * decimal-microsecond grid (1024 ≈ 1 us, 2048 ≈ 2 us, ... 2^24 ≈ 16 ms),
 * which lets wait_event_timing_bucket() avoid a /1000 on the hot path.
 *
 * 16 buckets cover, approximately: <1us, 1-2us, 2-4us, ... 8-16ms, >=16ms
 * (exact boundaries: 1024, 2048, 4096, ... 2^24 ns).
 */
#define WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS	16

/*
 * Compact per-class mapping for the flat events[] array.
 *
 * WAIT_EVENT_TIMING_RAW_CLASSES, WAIT_EVENT_TIMING_DENSE_CLASSES, and
 * WAIT_EVENT_TIMING_NUM_EVENTS are generated into wait_event_types.h by
 * generate-wait_event_types.pl from wait_event_names.txt.
 *
 * The mapping arrays (wait_event_class_dense, wait_event_class_nevents,
 * wait_event_class_offset, wait_event_dense_to_classid) and internal
 * helper functions are in wait_event_timing.c (included from the
 * generated wait_event_timing_data.h).
 */

/* Sentinel returned by wait_event_timing_index() for LWLock events */
#define WAIT_EVENT_TIMING_IDX_LWLOCK	(-2)

/*
 * Per-event accumulated statistics.  One entry per distinct wait event
 * per backend.  These are written only by the owning backend, so no
 * locking is needed.  External readers may see torn reads for 64-bit
 * fields on 32-bit platforms, but that is acceptable for statistics.
 */
typedef struct WaitEventTimingEntry
{
	int64		count;			/* number of occurrences */
	int64		total_ns;		/* total wait duration in nanoseconds */
	int64		max_ns;			/* longest single wait in nanoseconds */
	int64		histogram[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];
} WaitEventTimingEntry;

/*
 * LWLock-specific open-addressing hash table for unbounded tranche IDs.
 * Per-backend, written only by the owning backend -- no locking needed.
 * Tranche IDs are dynamically allocated by LWLockNewTrancheId() starting
 * at LWTRANCHE_FIRST_USER_DEFINED (~88) with no upper bound.  The hash
 * maps tranche_id -> dense index into lwlock_events[].
 */
/*
 * Hash slot count vs. entry cap.
 *
 * The cap on distinct LWLock tranches per backend (and the slot count
 * of the open-addressing hash that resolves them) is configured at
 * server start by the GUC wait_event_timing_max_tranches.  Default 192
 * matches real-world ceilings on deployments without many custom
 * extensions; raise it for installations that load many extensions
 * which register their own LWLock tranches.  See guc_parameters.dat.
 *
 * The slot count is derived as the next power of two of (2 ×
 * max_tranches), giving a load factor of at most 50% (typically ~37%
 * because the next-pow2 jump usually overshoots).  Linear probing gets
 * expensive fast above 50% load (avg ~8.5 probes on miss at 75%, ~1.6
 * at 37.5%), and this table sits inside the single-writer hot path in
 * pgstat_report_wait_end_timing, so probe length matters.  The slot-
 * table memory cost is small relative to the entry array (4 bytes per
 * slot vs. ~152 bytes per entry).
 *
 * Both the slot table (entries[]) and the dense events array
 * (lwlock_events[]) are sized at allocation time and stored in the
 * per-backend DSA region following the WaitEventTimingState header
 * for that backend; see the layout description there.  The
 * LWLockTimingHash struct below holds only the immutable size metadata
 * and the runtime num_used counter -- the arrays themselves are not
 * struct members because their length is runtime-determined.
 */

/*
 * Sentinel marking an empty hash slot.  We deliberately reserve the
 * upper end of the uint16 range (0xFFFF) instead of 0 so that any
 * legal LWLock tranche ID -- including the currently-unused tranche 0
 * (lwlocklist.h: "0 is available; was formerly BufFreelistLock") --
 * can be stored and matched correctly.  Keeping the sentinel decoupled
 * from the LWLock numbering makes this hash table robust to future
 * changes in lwlocklist.h.
 */
#define LWLOCK_TIMING_EMPTY_SLOT	((uint16) 0xFFFF)

typedef struct LWLockTimingHashEntry
{
	uint16		tranche_id;		/* LWLOCK_TIMING_EMPTY_SLOT (0xFFFF)
								 * marks an unoccupied slot.  Real
								 * tranche IDs are uint16 and use the
								 * remaining range. */
	uint16		dense_idx;		/* index into lwlock_events[] */
} LWLockTimingHashEntry;

/*
 * Header-only struct.  The actual hash slot array and dense events
 * array live in the per-backend DSA region immediately after the
 * WaitEventTimingState (in that order); their addresses are recovered
 * via wait_event_timing_lwlock_entries() / _lwlock_events() helpers
 * defined in wait_event_timing.c.
 */
typedef struct LWLockTimingHash
{
	int			num_used;		/* count of occupied entries */
	int			hash_size;		/* size of slot table (power of 2);
								 * immutable after allocation */
	int			max_entries;	/* cap on distinct tranches; immutable
								 * after allocation, == GUC value at
								 * postmaster start */
} LWLockTimingHash;

/* Declaration of the GUC (see guc_parameters.dat). */
extern PGDLLIMPORT int wait_event_timing_max_tranches;

/*
 * Per-backend wait event timing state.  Allocated in shared memory,
 * one per MaxBackends + NUM_AUXILIARY_PROCS slot.
 *
 * Synchronization: each slot is written exclusively by its owning backend.
 * Cross-backend readers (pg_stat_get_wait_event_timing) are lock-free and
 * tolerate torn reads of 64-bit fields on 32-bit platforms (acceptable for
 * statistics).  Cross-backend reset is request-based: the caller atomically
 * bumps reset_generation, and the owning backend observes the change on
 * its next wait_end and performs the reset itself.  This keeps the hot
 * path lock-free while guaranteeing atomic, race-free resets.
 *
 * DSA layout: each backend's slot is laid out as
 *
 *     [ WaitEventTimingState header ]
 *     [ LWLockTimingHashEntry[hash_size] ]
 *     [ WaitEventTimingEntry[max_entries]      <- lwlock_events[] ]
 *
 * where hash_size and max_entries are runtime-derived from the GUC
 * wait_event_timing_max_tranches and recorded in the
 * WaitEventTimingState->lwlock_hash header.  Slots are laid out
 * contiguously in the shared array using a runtime stride
 * (wait_event_timing_per_backend_stride in wait_event_timing.c) rather
 * than the C array-indexing operator [], because per-backend size is
 * determined at server start.
 */
typedef struct WaitEventTimingState
{
	/*
	 * Generation counter for cross-backend reset requests.  Incremented
	 * atomically by pg_stat_reset_wait_event_timing(target).  The owning
	 * backend tracks a local last-observed value; when it differs from the
	 * shared value, the owner performs the reset before the next event
	 * accumulation.  Pure request-response: no locks needed on any path.
	 */
	pg_atomic_uint32 reset_generation;

	/* Current wait start timestamp (set by pgstat_report_wait_start) */
	instr_time	wait_start;

	/* Current wait_event_info (cached for use in wait_end) */
	uint32		current_event;

	/*
	 * Counter of resets that have been *observed and acted on* by this
	 * backend.  Own-backend resets (pg_stat_reset_wait_event_timing(NULL)
	 * or own-pid) are synchronous and bump this once per call.
	 * Cross-backend resets COALESCE: if multiple resets are requested
	 * for this backend between two of its wait_ends, the owner observes
	 * them as one and bumps reset_count once.  Callers polling for "did
	 * my async reset land?" should rely on the N -> N+1 transition;
	 * do not use this column as a request counter.
	 */
	int64		reset_count;

	/* Per-event statistics: flat array for bounded classes */
	WaitEventTimingEntry events[WAIT_EVENT_TIMING_NUM_EVENTS];

	/* Per-event statistics: hash table for LWLock class (unbounded IDs) */
	LWLockTimingHash lwlock_hash;

	/* Count of LWLock events dropped because the LWLock-timing hash
	 * table reached its cap (the GUC wait_event_timing_max_tranches). */
	int64		lwlock_overflow_count;

	/* Count of flat array events dropped due to eventId exceeding slot count */
	int64		flat_overflow_count;
} WaitEventTimingState;


/*
 * Per-session wait event trace ring buffer (10046-style).
 * When wait_event_trace GUC is on for a session, every wait_end writes
 * a record to a per-backend ring buffer.  External tools read the buffer
 * via pg_get_backend_wait_event_trace().
 *
 * Query attribution is done by scanning the ring at read time: QUERY_START
 * and QUERY_END markers delimit which wait events belong to which query_id.
 * This eliminates the previous per-backend shared-memory hash table.
 *
 * The ring buffer is allocated lazily via DSA (Dynamic Shared Memory Areas)
 * on first use.  Only backends that enable wait_event_trace pay the ~4 MB
 * cost.  A small control struct in fixed shmem holds per-backend DSA pointers.
 */
#define WAIT_EVENT_TRACE_RING_SIZE	131072	/* must be power of 2, 128K records */

/* Trace record types */
#define TRACE_WAIT_EVENT	0
#define TRACE_QUERY_START	1
#define TRACE_QUERY_END		2
#define TRACE_EXEC_START	3
#define TRACE_EXEC_END		4

typedef struct WaitEventTraceRecord
{
	/*
	 * Seqlock for torn-read detection.  Writers set seq to an odd value
	 * before filling fields, then to even after.  Readers check seq before
	 * and after; if either is odd or they differ, the record is skipped.
	 *
	 * uint32 wraps after pos > 2^31 (~2.7 hours at 220K events/sec), but
	 * the protection only needs to hold for the reader's access window
	 * (~10-20 ns between seq_before and seq_after reads).  A collision
	 * requires advancing 2^31 positions in that window -- physically
	 * impossible by 11 orders of magnitude.
	 */
	uint32		seq;
	uint8		record_type;	/* TRACE_WAIT_EVENT / QUERY_START / QUERY_END */
	uint8		pad[3];
	int64		timestamp_ns;	/* monotonic clock */
	union
	{
		struct						/* record_type = TRACE_WAIT_EVENT */
		{
			uint32	event;			/* wait_event_info */
			uint32	pad2;
			int64	duration_ns;
		}			wait;
		struct						/* record_type = TRACE_QUERY_START/END */
		{
			int64	query_id;
			int64	pad2;
		}			query;
	}			data;
} WaitEventTraceRecord;			/* 32 bytes */

/*
 * Compile-time invariants for the trace ring.  These used to live as
 * prose in the header comment above; the asserts make accidental
 * violations (e.g. someone adding a field to WaitEventTraceRecord) a
 * build failure instead of a silently-broken ring.
 */
StaticAssertDecl(sizeof(WaitEventTraceRecord) == 32,
				 "WaitEventTraceRecord must be exactly 32 bytes: the "
				 "seqlock wrap-safety argument relies on single-record, "
				 "single-cache-line writes, and ARR_DATA_PTR / mask-index "
				 "math assumes a fixed record stride.");
StaticAssertDecl((WAIT_EVENT_TRACE_RING_SIZE & (WAIT_EVENT_TRACE_RING_SIZE - 1)) == 0,
				 "WAIT_EVENT_TRACE_RING_SIZE must be a power of two; "
				 "the ring uses mask indexing (pos & (SIZE - 1)).");
StaticAssertDecl(WAIT_EVENT_TRACE_RING_SIZE >= 2,
				 "WAIT_EVENT_TRACE_RING_SIZE must be >= 2 so that the "
				 "write_pos seqlock parity interleave yields distinct "
				 "records across neighbouring slots.");

typedef struct WaitEventTraceState
{
	pg_atomic_uint64 write_pos;	/* monotonically increasing, wraps via mask */
	WaitEventTraceRecord records[WAIT_EVENT_TRACE_RING_SIZE];
} WaitEventTraceState;
/* ~4 MB per backend (allocated lazily via DSA).  When the ring wraps,
 * old records are silently overwritten.  Readers detect overwritten
 * records via the seqlock (odd seq = in-flight write). */

/*
 * Control struct for lazy DSA-based trace ring allocation.
 * Lives in fixed shared memory, one per cluster.
 *
 * The per-backend trace ring is a lock-free transport for external consumers.
 * Writers (owning backend) claim slots via pg_atomic_fetch_add_u64 on
 * write_pos and use a per-record seqlock for torn-read detection.
 *
 * External readers (extensions, background workers reading another
 * backend's ring) should:
 *
 * 1. Acquire WaitEventTraceCtl->lock in LW_SHARED before resolving
 *    trace_ptrs[procNumber] via dsa_get_address.  This prevents the
 *    owning backend's wait_event_trace_release_slot from dsa_free()'ing
 *    the chunk while you hold a pointer into it.
 *
 * 2. Hold the LWLock only long enough to read write_pos and copy the
 *    relevant slice of records[] into local memory.  Release the lock
 *    BEFORE doing per-record processing.  Holding the lock during the
 *    full processing path turns every other backend's first-time
 *    wait_event_trace_attach (e.g. new connections under cluster-wide
 *    wait_event_capture = trace) into a wait that scales with your
 *    processing speed -- not acceptable on OLTP workloads.  The
 *    snapshot-then-process pattern caps the lock-hold time at memcpy
 *    bandwidth (~1 ms for the full 4 MB ring; sub-ms for incremental
 *    polls that copy only the delta since the last poll).
 *
 * 3. Use the seqlock protocol on each record copied: read rec->seq
 *    before and after copying fields; skip the record if either value
 *    is odd or they differ.  This catches concurrent writes by the
 *    owning backend (which never takes the lock above -- the writer
 *    is purely lock-free, see pgstat_report_wait_end_timing).
 *
 * 4. Track write_pos between polls and copy only new records, so the
 *    snapshot under the lock stays small even when scanning many
 *    backends.
 *
 * Same-backend readers (the in-tree pg_get_backend_wait_event_trace SRF)
 * do NOT use the LWLock above -- same-backend serialization is implicit
 * because a backend can only run one command at a time, and the SRF
 * coordinates with wait_event_trace_release_slot via per-backend flags.
 * That mechanism is private to wait_event_timing.c; external code should
 * use the cross-backend protocol described above.
 */
typedef struct WaitEventTraceControl
{
	dsa_handle	trace_dsa_handle;	/* DSA_HANDLE_INVALID until first use */
	LWLock		lock;				/* protects DSA creation */
	dsa_pointer trace_ptrs[FLEXIBLE_ARRAY_MEMBER]; /* per procNumber */
} WaitEventTraceControl;


/*
 * Capture levels for the wait_event_capture GUC.  Order is significant:
 * higher values are strict supersets of lower ones, and code paths use
 * "level >= WAIT_EVENT_CAPTURE_STATS" to test for activation.
 *
 *   OFF   - No instrumentation, no hot-path cost.
 *   STATS - Aggregated per-event statistics in pg_stat_wait_event_timing
 *           (counts, durations, histograms).  Hot path samples wall time
 *           around every wait.
 *   TRACE - Everything in STATS plus a per-session ring buffer of
 *           individual events and query markers, exposed via
 *           pg_backend_wait_event_trace.  Adds ~4 MB DSA per session.
 */
typedef enum WaitEventCaptureLevel
{
	WAIT_EVENT_CAPTURE_OFF = 0,
	WAIT_EVENT_CAPTURE_STATS,
	WAIT_EVENT_CAPTURE_TRACE,
}			WaitEventCaptureLevel;

/* GUC variable */
extern PGDLLIMPORT int wait_event_capture;

/* Pointer to this backend's timing state in shared memory */
extern PGDLLIMPORT WaitEventTimingState *my_wait_event_timing;

/* Pointer to this backend's trace ring buffer in shared memory */
extern PGDLLIMPORT WaitEventTraceState *my_wait_event_trace;

/* This backend's procNumber for the trace ring, or -1 if not set */
extern PGDLLIMPORT int my_trace_proc_number;

/*
 * Shared memory setup -- registered via the shmem subsystem registry
 * (src/include/storage/subsystemlist.h).  Stub builds expose a no-op
 * callbacks struct so subsystemlist.h references resolve either way.
 */
extern PGDLLIMPORT const ShmemCallbacks WaitEventTimingShmemCallbacks;
extern PGDLLIMPORT const ShmemCallbacks WaitEventTraceControlShmemCallbacks;

/* Called from InitProcess() to point my_wait_event_timing at our slot */
extern void pgstat_set_wait_event_timing_storage(int procNumber);
extern void pgstat_reset_wait_event_timing_storage(void);

/* Lazy DSA-based trace ring buffer allocation */
extern void wait_event_trace_ensure_dsa(void);
extern void wait_event_trace_attach(int procNumber);
extern void wait_event_trace_detach(int procNumber);

/* GUC hooks declared in guc_hooks.h */

/* Trace marker functions (defined in wait_event_timing.c) */
extern void wait_event_trace_query_start(int64 query_id);
extern void wait_event_trace_query_end(int64 query_id);
extern void wait_event_trace_exec_start(int64 query_id);
extern void wait_event_trace_exec_end(int64 query_id);

#endif							/* WAIT_EVENT_TIMING_H */
