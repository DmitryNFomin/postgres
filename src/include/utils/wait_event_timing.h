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
#include "utils/wait_classes.h"

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
 * Wait event classes (from wait_classes.h) use sparse classId values
 * (0x01, 0x03-0x0B).  LWLock (0x01) uses a separate hash table because
 * tranche IDs are dynamically allocated and unbounded.  Classes 0x00
 * and 0x02 do not exist.
 *
 * Instead of a uniform 256 slots per class (wasting memory on unused
 * classes and oversized per-class ranges), we use per-class slot counts
 * sized to actual event populations with headroom:
 *
 *   Dense  ClassId  Class           Actual  Slots  Offset
 *     0     0x03    Lock              12      32       0
 *     1     0x04    Buffer             4      16      32
 *     2     0x05    Activity          18      32      48
 *     3     0x06    Client            12      32      80
 *     4     0x07    Extension        128     128     112
 *     5     0x08    IPC               57     128     240
 *     6     0x09    Timeout           11      32     368
 *     7     0x0A    IO                83     128     400
 *     8     0x0B    InjectionPoint   128     128     528
 *                                          Total:   656
 *
 * Per-backend flat array: 656 * 152 bytes = ~97 KB (vs ~456 KB before).
 */
#define WAIT_EVENT_TIMING_RAW_CLASSES	12	/* 0x00 .. 0x0B for bounds check */
#define WAIT_EVENT_TIMING_DENSE_CLASSES	9	/* actual classes in the flat array */
#define WAIT_EVENT_TIMING_NUM_EVENTS	656	/* sum of per-class slot counts */

/*
 * Maps raw classId (0x00..0x0B) to dense class index, or -1 for unused /
 * LWLock classes.  Indexed by (wait_event_info >> 24) & 0xFF.
 */
static const int8 wait_event_class_dense[WAIT_EVENT_TIMING_RAW_CLASSES] = {
	-1,		/* 0x00: unused */
	-1,		/* 0x01: LWLock (uses hash) */
	-1,		/* 0x02: unused */
	 0,		/* 0x03: Lock */
	 1,		/* 0x04: Buffer */
	 2,		/* 0x05: Activity */
	 3,		/* 0x06: Client */
	 4,		/* 0x07: Extension */
	 5,		/* 0x08: IPC */
	 6,		/* 0x09: Timeout */
	 7,		/* 0x0A: IO */
	 8,		/* 0x0B: InjectionPoint */
};

/* Per dense-class: maximum eventId (exclusive) */
static const int wait_event_class_nevents[WAIT_EVENT_TIMING_DENSE_CLASSES] = {
	32,		/* Lock */
	16,		/* Buffer */
	32,		/* Activity */
	32,		/* Client */
	128,	/* Extension */
	128,	/* IPC */
	32,		/* Timeout */
	128,	/* IO */
	128,	/* InjectionPoint */
};

/* Per dense-class: cumulative offset into events[] */
static const int wait_event_class_offset[WAIT_EVENT_TIMING_DENSE_CLASSES] = {
	0,		/* Lock */
	32,		/* Buffer */
	48,		/* Activity */
	80,		/* Client */
	112,	/* Extension */
	240,	/* IPC */
	368,	/* Timeout */
	400,	/* IO */
	528,	/* InjectionPoint */
};

/* Reverse mapping: dense class index -> raw classId */
static const uint8 wait_event_dense_to_classid[WAIT_EVENT_TIMING_DENSE_CLASSES] = {
	0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
};

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
							 * (event & 0xFFFF) */
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
	pg_atomic_uint64 write_pos;	/* monotonically increasing write position */
	WaitEventTraceRecord records[WAIT_EVENT_TRACE_RING_SIZE];
} WaitEventTraceState;			/* ~4 MB per backend (allocated lazily via DSA) */

/*
 * Control struct for lazy DSA-based trace ring allocation.
 * Lives in fixed shared memory, one per cluster.
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


/*
 * Convert wait_event_info to a flat index for the events[] array.
 * Returns WAIT_EVENT_TIMING_IDX_LWLOCK (-2) for LWLock class events;
 * the caller must use lwlock_timing_lookup() for those.
 * Returns -1 for out-of-range or unknown class events.
 */
static inline int
wait_event_timing_index(uint32 wait_event_info)
{
	int			classId = (wait_event_info >> 24) & 0xFF;
	int			eventId = wait_event_info & 0xFFFF;
	int			dense;

	/* LWLock tranche IDs are unbounded -- use hash table */
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
 * Open-addressing with linear probing.  Returns NULL if hash is full.
 */
static inline WaitEventTimingEntry *
lwlock_timing_lookup(LWLockTimingHash *ht, uint16 tranche_id)
{
	uint32		hash = (uint32) tranche_id * 2654435761U; /* Knuth multiplicative */
	int			slot = hash & (LWLOCK_TIMING_HASH_SIZE - 1);
	int			i;

	for (i = 0; i < LWLOCK_TIMING_HASH_SIZE; i++)
	{
		LWLockTimingHashEntry *e = &ht->entries[slot];

		if (e->tranche_id == tranche_id)
			return &ht->lwlock_events[e->dense_idx];

		if (e->tranche_id == 0)
		{
			/* Empty slot -- insert if room */
			if (ht->num_used >= LWLOCK_TIMING_MAX_ENTRIES)
				return NULL;	/* hash full, drop this event */

			e->tranche_id = tranche_id;
			e->dense_idx = ht->num_used++;
			return &ht->lwlock_events[e->dense_idx];
		}

		slot = (slot + 1) & (LWLOCK_TIMING_HASH_SIZE - 1);
	}

	return NULL;				/* should not happen with load factor < 1 */
}

/*
 * Compute histogram bucket index for a duration in nanoseconds.
 * Uses log2 bucketing in microseconds:
 *   bucket 0: [0, 1) us
 *   bucket 1: [1, 2) us
 *   bucket 2: [2, 4) us
 *   ...
 *   bucket 15: [16384, inf) us  (>= 16ms)
 */
static inline int
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
 * Write a QUERY_START marker into the trace ring buffer.
 * Called from pgstat_report_query_id() when query_id transitions to non-zero.
 */
static inline void
wait_event_trace_query_start(int64 query_id)
{
	if (unlikely(wait_event_trace && my_wait_event_trace != NULL && query_id != 0))
	{
		uint64	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
		WaitEventTraceRecord *rec =
			&my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		uint32	seq = (uint32)(pos * 2 + 1);
		instr_time now;

		rec->seq = seq;
		pg_write_barrier();

		INSTR_TIME_SET_CURRENT(now);
		rec->record_type = TRACE_QUERY_START;
		rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
		rec->data.query.query_id = query_id;
		rec->data.query.pad2 = 0;

		pg_write_barrier();
		rec->seq = seq + 1;
	}
}

/*
 * Write a QUERY_END marker into the trace ring buffer.
 * Called from pgstat_report_query_id() when query_id transitions to zero.
 */
static inline void
wait_event_trace_query_end(int64 query_id)
{
	if (unlikely(wait_event_trace && my_wait_event_trace != NULL && query_id != 0))
	{
		uint64	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
		WaitEventTraceRecord *rec =
			&my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		uint32	seq = (uint32)(pos * 2 + 1);
		instr_time now;

		rec->seq = seq;
		pg_write_barrier();

		INSTR_TIME_SET_CURRENT(now);
		rec->record_type = TRACE_QUERY_END;
		rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
		rec->data.query.query_id = query_id;
		rec->data.query.pad2 = 0;

		pg_write_barrier();
		rec->seq = seq + 1;
	}
}

/*
 * Write an EXEC_START marker into the trace ring buffer.
 * Called from ExecutorStart() to separate planning from execution phase.
 */
static inline void
wait_event_trace_exec_start(int64 query_id)
{
	if (unlikely(wait_event_trace && my_wait_event_trace != NULL && query_id != 0))
	{
		uint64	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
		WaitEventTraceRecord *rec =
			&my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		uint32	seq = (uint32)(pos * 2 + 1);
		instr_time now;

		rec->seq = seq;
		pg_write_barrier();

		INSTR_TIME_SET_CURRENT(now);
		rec->record_type = TRACE_EXEC_START;
		rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
		rec->data.query.query_id = query_id;
		rec->data.query.pad2 = 0;

		pg_write_barrier();
		rec->seq = seq + 1;
	}
}

#endif							/* WAIT_EVENT_TIMING_H */
