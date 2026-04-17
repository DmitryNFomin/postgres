#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "pgstat.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"
#include "utils/wait_event_types.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/*
 * stress_wait_events(n int) -> bigint
 *
 * Calls pgstat_report_wait_start()/pgstat_report_wait_end() in a tight loop
 * n times.  Returns the elapsed time in microseconds.
 *
 * This measures the pure overhead of the wait event timing instrumentation:
 *   - 2x clock_gettime(CLOCK_MONOTONIC) via VDSO per iteration
 *   - 1x histogram bucket calculation (CLZ instruction)
 *   - 1x accumulator update (counter + total_ns)
 *   - optionally 1x trace ring buffer write
 *
 * Usage:
 *   SELECT stress_wait_events(1000000);  -- 1M iterations
 *   -- returns elapsed microseconds
 *   -- overhead per iteration = result / 1000000 microseconds
 */
PG_FUNCTION_INFO_V1(stress_wait_events);

Datum
stress_wait_events(PG_FUNCTION_ARGS)
{
	int32		iterations = PG_GETARG_INT32(0);
	instr_time	start,
				end;
	int			i;

	if (iterations < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iterations must be non-negative")));

	INSTR_TIME_SET_CURRENT(start);

	for (i = 0; i < iterations; i++)
	{
		pgstat_report_wait_start(WAIT_EVENT_PG_SLEEP);
		pgstat_report_wait_end();
	}

	INSTR_TIME_SET_CURRENT(end);

	PG_RETURN_INT64(INSTR_TIME_GET_MICROSEC(end) - INSTR_TIME_GET_MICROSEC(start));
}

/*
 * test_lwlock_hash_overflow(n_tranches int) -> int
 *
 * Registers n_tranches custom LWLock tranches and triggers a
 * pgstat_report_wait_start()/pgstat_report_wait_end() cycle on each.
 * Returns the number of tranches that were triggered.
 *
 * With n_tranches > LWLOCK_TIMING_MAX_ENTRIES (192), this exercises the
 * hash overflow path and verifies the one-time WARNING fires.
 *
 * Usage:
 *   SET wait_event_timing = on;
 *   SET client_min_messages = warning;
 *   SELECT test_lwlock_hash_overflow(200);
 *   -- expect WARNING about LWLock hash table full
 */
PG_FUNCTION_INFO_V1(test_lwlock_hash_overflow);

Datum
test_lwlock_hash_overflow(PG_FUNCTION_ARGS)
{
	int32		n_tranches = PG_GETARG_INT32(0);
	int			i;
	char		name[64];

	if (n_tranches < 0 || n_tranches > 1000)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("n_tranches must be between 0 and 1000")));

	for (i = 0; i < n_tranches; i++)
	{
		int		tranche_id;
		uint32	event;

		tranche_id = LWLockNewTrancheId();
		snprintf(name, sizeof(name), "test_lwlock_overflow_%d", i);
		LWLockRegisterTranche(tranche_id, name);

		/* Construct wait_event_info: PG_WAIT_LWLOCK | tranche_id */
		event = PG_WAIT_LWLOCK | (uint32) tranche_id;

		pgstat_report_wait_start(event);
		pgstat_report_wait_end();
	}

	PG_RETURN_INT32(n_tranches);
}
