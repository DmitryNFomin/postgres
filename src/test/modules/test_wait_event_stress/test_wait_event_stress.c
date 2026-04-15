#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "pgstat.h"
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
