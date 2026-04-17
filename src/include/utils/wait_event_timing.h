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
#include "utils/dsa.h"
#include "utils/wait_event_types.h"

/*
 * Number of log2 histogram buckets.  Bucket i covers durations in
 * [2^i, 2^(i+1)) microseconds, except bucket 0 which covers [0, 1) us
 * and the last bucket which covers [2^(NBUCKETS-1), infinity).
 *
 * 16 buckets cover: <1us, 1-2us, 2-4us, ... 8-16ms, >=16ms
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
 * generated wait_event_timing_data.c).
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
#define LWLOCK_TIMING_HASH_SIZE		256		/* must be power of 2 */
#define LWLOCK_TIMING_MAX_ENTRIES	192		/* ~75% load factor */

typedef struct LWLockTimingHashEntry
{
	uint16		tranche_id;		/* 0 = empty slot; 16-bit matches
							 * core wait_event_info encoding
							 * (event & 0xFFFF).  Theoretical
							 * limit of 65535 tranches is
							 * academic -- real deployments
							 * use <200. */
	uint16		dense_idx;		/* index into lwlock_events[] */
} LWLockTimingHashEntry;

typedef struct LWLockTimingHash
{
	int			num_used;
	LWLockTimingHashEntry entries[LWLOCK_TIMING_HASH_SIZE];
	WaitEventTimingEntry lwlock_events[LWLOCK_TIMING_MAX_ENTRIES];
} LWLockTimingHash;

/*
 * Per-backend wait event timing state.  Allocated in shared memory,
 * one per MaxBackends slot.
 */
typedef struct WaitEventTimingState
{
	/* Per-backend lock for cross-backend reset (pgstat_reset_entry pattern) */
	LWLock		lock;

	/* Current wait start timestamp (set by pgstat_report_wait_start) */
	instr_time	wait_start;

	/* Current wait_event_info (cached for use in wait_end) */
	uint32		current_event;

	/* Reset counter -- incremented by pg_stat_reset_wait_event_timing() */
	int64		reset_count;

	/* Per-event statistics: flat array for bounded classes */
	WaitEventTimingEntry events[WAIT_EVENT_TIMING_NUM_EVENTS];

	/* Per-event statistics: hash table for LWLock class (unbounded IDs) */
	LWLockTimingHash lwlock_hash;

	/* Count of LWLock events dropped due to hash table overflow (192 limit) */
	int64		lwlock_overflow_count;

	/* Count of flat array events dropped due to eventId exceeding slot count */
	int64		flat_overflow_count;
} WaitEventTimingState;


/*
 * Per-session wait event trace ring buffer (10046-style).
 * When wait_event_trace GUC is on for a session, every wait_end writes
 * a record to a per-backend ring buffer.  External tools read the buffer
 * via pg_stat_get_wait_event_trace().
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
 * External readers (extensions, background workers) should:
 * 1. Acquire WaitEventTraceCtl->lock in LW_SHARED before resolving
 *    trace_ptrs[procNumber] via dsa_get_address, and hold it for the
 *    duration of the scan.  This prevents the owning backend from
 *    freeing the DSA chunk during the read.
 * 2. Use the seqlock protocol: read rec->seq before and after copying
 *    fields; skip the record if either value is odd or they differ.
 * 3. Track write_pos between polls to process only new records.
 */
typedef struct WaitEventTraceControl
{
	dsa_handle	trace_dsa_handle;	/* DSA_HANDLE_INVALID until first use */
	LWLock		lock;				/* protects DSA creation */
	dsa_pointer trace_ptrs[FLEXIBLE_ARRAY_MEMBER]; /* per procNumber */
} WaitEventTraceControl;


/* GUC variables */
extern PGDLLIMPORT bool wait_event_timing;
extern PGDLLIMPORT bool wait_event_trace;

/* Pointer to this backend's timing state in shared memory */
extern PGDLLIMPORT WaitEventTimingState *my_wait_event_timing;

/* Pointer to this backend's trace ring buffer in shared memory */
extern PGDLLIMPORT WaitEventTraceState *my_wait_event_trace;

/* Shared memory setup */
extern Size WaitEventTimingShmemSize(void);
extern void WaitEventTimingShmemInit(void);
extern Size WaitEventTraceControlShmemSize(void);
extern void WaitEventTraceControlShmemInit(void);

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
