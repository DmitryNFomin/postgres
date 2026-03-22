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
 * Controlled by the wait_event_timing GUC (default: off).
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/wait_event_timing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"
#include "utils/wait_event_timing.h"

/* GUC variable */
bool		wait_event_timing = false;

/* Pointer to this backend's timing state */
WaitEventTimingState *my_wait_event_timing = NULL;

/* Shared memory base pointer (array of MaxBackends entries) */
static WaitEventTimingState *WaitEventTimingArray = NULL;

/*
 * Report the shared memory space needed.
 */
Size
WaitEventTimingShmemSize(void)
{
	return mul_size(MaxBackends, sizeof(WaitEventTimingState));
}

/*
 * Initialize shared memory for wait event timing.
 */
void
WaitEventTimingShmemInit(void)
{
	bool		found;
	Size		size;

	size = WaitEventTimingShmemSize();

	WaitEventTimingArray = (WaitEventTimingState *)
		ShmemInitStruct("WaitEventTimingArray", size, &found);

	if (!found)
		memset(WaitEventTimingArray, 0, size);
}

/*
 * Point my_wait_event_timing at this backend's slot.
 * Called from InitProcess() after the backend has a valid procNumber.
 *
 * procNumber is the PGPROC array index (from GetNumberFromPGProc).
 */
void
pgstat_set_wait_event_timing_storage(int procNumber)
{
	Assert(WaitEventTimingArray != NULL);
	Assert(procNumber >= 0 && procNumber < MaxBackends);

	my_wait_event_timing = &WaitEventTimingArray[procNumber];

	/* Zero the state for this new backend session */
	memset(my_wait_event_timing, 0, sizeof(WaitEventTimingState));
}

/*
 * Detach from timing state on backend exit.
 */
void
pgstat_reset_wait_event_timing_storage(void)
{
	my_wait_event_timing = NULL;
}

/*
 * SQL function: pg_stat_get_wait_event_timing(OUT ...)
 *
 * Returns one row per (backend_id, wait_event) with non-zero counts.
 */
Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	if (WaitEventTimingArray == NULL)
		PG_RETURN_VOID();

	for (backend_idx = 0; backend_idx < MaxBackends; backend_idx++)
	{
		WaitEventTimingState *state = &WaitEventTimingArray[backend_idx];
		int			i;

		for (i = 0; i < WAIT_EVENT_TIMING_NUM_EVENTS; i++)
		{
			WaitEventTimingEntry *entry = &state->events[i];
			Datum		values[8];
			bool		nulls[8];
			uint32		wait_event_info;
			const char *event_type;
			const char *event_name;
			int			bucket;

			if (entry->count == 0)
				continue;

			/* Reconstruct wait_event_info from flat index */
			wait_event_info = ((i / WAIT_EVENT_TIMING_EVENTS_PER_CLASS) << 24) |
				(i % WAIT_EVENT_TIMING_EVENTS_PER_CLASS);

			event_type = pgstat_get_wait_event_type(wait_event_info);
			event_name = pgstat_get_wait_event(wait_event_info);

			if (event_type == NULL || event_name == NULL)
				continue;

			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(backend_idx + 1);  /* backend_id (1-based) */
			values[1] = CStringGetTextDatum(event_type);
			values[2] = CStringGetTextDatum(event_name);
			values[3] = Int64GetDatum(entry->count);
			values[4] = Float8GetDatum((double) entry->total_ns / 1000000.0);  /* ms */
			values[5] = Float8GetDatum(entry->count > 0
									   ? (double) entry->total_ns / entry->count / 1000.0
									   : 0.0);  /* avg us */
			values[6] = Float8GetDatum((double) entry->max_ns / 1000.0);  /* max us */

			/* Pack histogram into a text representation */
			{
				StringInfoData buf;

				initStringInfo(&buf);
				appendStringInfoChar(&buf, '{');
				for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
				{
					if (bucket > 0)
						appendStringInfoChar(&buf, ',');
					appendStringInfo(&buf, "%d", entry->histogram[bucket]);
				}
				appendStringInfoChar(&buf, '}');
				values[7] = CStringGetTextDatum(buf.data);
				pfree(buf.data);
			}

			tuplestore_putvalues(rsinfo->setResult,
								rsinfo->setDesc,
								values, nulls);
		}
	}

	PG_RETURN_VOID();
}
