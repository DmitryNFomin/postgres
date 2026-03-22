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
 * External tools can read the accumulated stats from shared memory, or
 * use the pg_wait_event_timing view.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/wait_event_timing.h
 *-------------------------------------------------------------------------
 */
#ifndef WAIT_EVENT_TIMING_H
#define WAIT_EVENT_TIMING_H

#include "port/pg_bitutils.h"
#include "portability/instr_time.h"

/*
 * Number of log2 histogram buckets.  Bucket i covers durations in
 * [2^i, 2^(i+1)) microseconds, except bucket 0 which covers [0, 1) us
 * and the last bucket which covers [2^(NBUCKETS-1), infinity).
 *
 * 16 buckets cover: <1us, 1-2us, 2-4us, ... 8-16ms, >=16ms
 */
#define WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS	16

/*
 * Maximum number of distinct wait events we track.  This must be large
 * enough to cover all built-in events (currently ~288) plus some headroom
 * for custom extension events.  The wait event ID (low 16 bits of
 * wait_event_info) is used as the index within a class, and we store
 * per-class arrays.
 *
 * Wait event classes and their max event counts:
 *   LWLock (0x01): up to 256 tranches (dynamic, use separate array)
 *   Lock (0x03): ~10 lock types
 *   Buffer (0x04): ~6 events
 *   Activity (0x05): ~20 events
 *   Client (0x06): ~14 events
 *   Extension (0x07): up to 128 custom events
 *   IPC (0x08): ~59 events
 *   Timeout (0x09): ~13 events
 *   IO (0x0A): ~85 events
 *   InjectionPoint (0x0B): up to 128 custom events
 *
 * We use a flat array indexed by (classId >> 24) * 256 + eventId.
 * With 12 classes * 256 = 3072 slots, this uses ~73 KB per backend.
 */
#define WAIT_EVENT_TIMING_CLASSES		12  /* 0x00 .. 0x0B */
#define WAIT_EVENT_TIMING_EVENTS_PER_CLASS	256
#define WAIT_EVENT_TIMING_NUM_EVENTS \
	(WAIT_EVENT_TIMING_CLASSES * WAIT_EVENT_TIMING_EVENTS_PER_CLASS)

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
	int32		histogram[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];
} WaitEventTimingEntry;

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

	/* Reset counter — incremented by pg_stat_reset_wait_event_timing() */
	int64		reset_count;

	/* Per-event statistics array */
	WaitEventTimingEntry events[WAIT_EVENT_TIMING_NUM_EVENTS];
} WaitEventTimingState;


/* GUC variable */
extern PGDLLIMPORT bool wait_event_timing;

/* Pointer to this backend's timing state in shared memory */
extern PGDLLIMPORT WaitEventTimingState *my_wait_event_timing;

/* Shared memory setup */
extern Size WaitEventTimingShmemSize(void);
extern void WaitEventTimingShmemInit(void);

/* Called from InitProcess() to point my_wait_event_timing at our slot */
extern void pgstat_set_wait_event_timing_storage(int procNumber);
extern void pgstat_reset_wait_event_timing_storage(void);


/* Convert wait_event_info to a flat index for the events[] array */
static inline int
wait_event_timing_index(uint32 wait_event_info)
{
	int			classId = (wait_event_info >> 24) & 0xFF;
	int			eventId = wait_event_info & 0xFFFF;

	if (unlikely(classId >= WAIT_EVENT_TIMING_CLASSES ||
				 eventId >= WAIT_EVENT_TIMING_EVENTS_PER_CLASS))
		return -1;

	return classId * WAIT_EVENT_TIMING_EVENTS_PER_CLASS + eventId;
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

	/* pg_leftmost_one_pos64 returns the position of the highest set bit */
	bucket = pg_leftmost_one_pos64((uint64) duration_us) + 1;

	if (bucket >= WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS)
		bucket = WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS - 1;

	return bucket;
}

#endif							/* WAIT_EVENT_TIMING_H */
