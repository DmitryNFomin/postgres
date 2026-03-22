/*-------------------------------------------------------------------------
 * wait_event.h
 *	  Definitions related to wait event reporting
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/wait_event.h
 * ----------
 */
#ifndef WAIT_EVENT_H
#define WAIT_EVENT_H

/* enums for wait events */
#include "utils/wait_event_types.h"
#include "utils/wait_event_timing.h"

extern const char *pgstat_get_wait_event(uint32 wait_event_info);
extern const char *pgstat_get_wait_event_type(uint32 wait_event_info);
static inline void pgstat_report_wait_start(uint32 wait_event_info);
static inline void pgstat_report_wait_end(void);
extern void pgstat_set_wait_event_storage(uint32 *wait_event_info);
extern void pgstat_reset_wait_event_storage(void);

extern PGDLLIMPORT uint32 *my_wait_event_info;


/*
 * Wait Events - Extension, InjectionPoint
 *
 * Use InjectionPoint when the server process is waiting in an injection
 * point.  Use Extension for other cases of the server process waiting for
 * some condition defined by an extension module.
 *
 * Extensions can define their own wait events in these categories.  They
 * should call one of these functions with a wait event string.  If the wait
 * event associated to a string is already allocated, it returns the wait
 * event information to use.  If not, it gets one wait event ID allocated from
 * a shared counter, associates the string to the ID in the shared dynamic
 * hash and returns the wait event information.
 *
 * The ID retrieved can be used with pgstat_report_wait_start() or equivalent.
 */
extern uint32 WaitEventExtensionNew(const char *wait_event_name);
extern uint32 WaitEventInjectionPointNew(const char *wait_event_name);

extern void WaitEventCustomShmemInit(void);
extern Size WaitEventCustomShmemSize(void);
extern char **GetWaitEventCustomNames(uint32 classId, int *nwaitevents);

/* ----------
 * pgstat_report_wait_start() -
 *
 *	Called from places where server process needs to wait.  This is called
 *	to report wait event information.  The wait information is stored
 *	as 4-bytes where first byte represents the wait event class (type of
 *	wait, for different types of wait, refer WaitClass) and the next
 *	3-bytes represent the actual wait event.  Currently 2-bytes are used
 *	for wait event which is sufficient for current usage, 1-byte is
 *	reserved for future usage.
 *
 *	Historically we used to make this reporting conditional on
 *	pgstat_track_activities, but the check for that seems to add more cost
 *	than it saves.
 *
 *	my_wait_event_info initially points to local memory, making it safe to
 *	call this before MyProc has been initialized.
 *
 *	When wait_event_timing is enabled, we also record the start timestamp
 *	for later duration computation in pgstat_report_wait_end().
 * ----------
 */
static inline void
pgstat_report_wait_start(uint32 wait_event_info)
{
	/*
	 * Since this is a four-byte field which is always read and written as
	 * four-bytes, updates are atomic.
	 */
	*(volatile uint32 *) my_wait_event_info = wait_event_info;

	/*
	 * Record start timestamp when wait_event_timing is enabled.
	 * The branch is predictable (always taken or never taken for the
	 * lifetime of the session) and costs ~1 ns when not taken.
	 */
	if (wait_event_timing && likely(my_wait_event_timing != NULL))
	{
		INSTR_TIME_SET_CURRENT(my_wait_event_timing->wait_start);
		my_wait_event_timing->current_event = wait_event_info;
	}
}

/* ----------
 * pgstat_report_wait_end() -
 *
 *	Called to report end of a wait.
 *
 *	When wait_event_timing is enabled, computes the wait duration and
 *	accumulates it into per-event statistics.
 * ----------
 */
static inline void
pgstat_report_wait_end(void)
{
	/*
	 * When timing is enabled, compute duration and accumulate stats
	 * before clearing the wait event.
	 */
	if (wait_event_timing && likely(my_wait_event_timing != NULL))
	{
		uint32		event = my_wait_event_timing->current_event;

		if (event != 0 && !INSTR_TIME_IS_ZERO(my_wait_event_timing->wait_start))
		{
			instr_time	now;
			int64		duration_ns;
			int			idx;

			INSTR_TIME_SET_CURRENT(now);
			duration_ns = INSTR_TIME_GET_NANOSEC(now) -
				INSTR_TIME_GET_NANOSEC(my_wait_event_timing->wait_start);

			idx = wait_event_timing_index(event);
			if (likely(idx >= 0))
			{
				WaitEventTimingEntry *entry = &my_wait_event_timing->events[idx];

				entry->count++;
				entry->total_ns += duration_ns;
				if (duration_ns > entry->max_ns)
					entry->max_ns = duration_ns;
				entry->histogram[wait_event_timing_bucket(duration_ns)]++;
			}

			INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
		}
	}

	/* see pgstat_report_wait_start() */
	*(volatile uint32 *) my_wait_event_info = 0;
}


#endif							/* WAIT_EVENT_H */
