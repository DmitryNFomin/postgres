/*--------------------------------------------------------------------------
 *
 * test_wait_primitive.c
 *		Microbenchmarks for PostgreSQL wait primitives.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_wait_primitive/test_wait_primitive.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "common/file_utils.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_wait_primitive_latch_set);
PG_FUNCTION_INFO_V1(test_wait_primitive_latch_timeout);
PG_FUNCTION_INFO_V1(test_wait_primitive_file_read);
PG_FUNCTION_INFO_V1(test_wait_primitive_usleep0);
PG_FUNCTION_INFO_V1(test_wait_primitive_report_only);

/* Make the results of calls in timed loops observable to the compiler. */
static volatile uint64 wait_primitive_sink;

static int64
get_iterations(FunctionCallInfo fcinfo)
{
	int64		iterations = PG_GETARG_INT64(0);

	if (iterations <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iteration count must be greater than zero")));

	return iterations;
}

static double
nanoseconds_per_iteration(instr_time start, instr_time end, int64 iterations)
{
	INSTR_TIME_SUBTRACT(end, start);
	return (double) INSTR_TIME_GET_NANOSEC(end) / (double) iterations;
}

Datum
test_wait_primitive_latch_set(PG_FUNCTION_ARGS)
{
	int64		iterations = get_iterations(fcinfo);
	uint64		sink = 0;
	instr_time start;
	instr_time end;

	SetLatch(MyLatch);
	INSTR_TIME_SET_CURRENT(start);
	for (int64 i = 0; i < iterations; i++)
		sink += WaitLatch(MyLatch,
						  WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						  0, WAIT_EVENT_PG_SLEEP);
	INSTR_TIME_SET_CURRENT(end);
	ResetLatch(MyLatch);

	wait_primitive_sink ^= sink;
	PG_RETURN_FLOAT8(nanoseconds_per_iteration(start, end, iterations));
}

Datum
test_wait_primitive_latch_timeout(PG_FUNCTION_ARGS)
{
	int64		iterations = get_iterations(fcinfo);
	uint64		sink = 0;
	instr_time start;
	instr_time end;

	ResetLatch(MyLatch);
	INSTR_TIME_SET_CURRENT(start);
	for (int64 i = 0; i < iterations; i++)
		sink += WaitLatch(MyLatch,
						  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						  0, WAIT_EVENT_PG_SLEEP);
	INSTR_TIME_SET_CURRENT(end);

	wait_primitive_sink ^= sink;
	PG_RETURN_FLOAT8(nanoseconds_per_iteration(start, end, iterations));
}

Datum
test_wait_primitive_file_read(PG_FUNCTION_ARGS)
{
	int64		iterations = get_iterations(fcinfo);
	char		tempdir[MAXPGPATH];
	char		path[MAXPGPATH];
	char	   *buffer;
	File		file;
	uint64		sink = 0;
	instr_time start;
	instr_time end;

	TempTablespacePath(tempdir, MyDatabaseTableSpace);
	snprintf(path, sizeof(path), "%s/%stest_wait_primitive.%d",
			 tempdir, PG_TEMP_FILE_PREFIX, MyProcPid);
	file = PathNameOpenFile(path, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (file < 0 && errno == ENOENT)
	{
		/* A new cluster may not have needed its temp directory yet. */
		(void) MakePGDirectory(tempdir);
		file = PathNameOpenFile(path,
								O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	}
	if (file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create temporary file \"%s\": %m", path)));

	buffer = palloc0(BLCKSZ);
	if (FileWrite(file, buffer, BLCKSZ, 0,
				  WAIT_EVENT_DATA_FILE_WRITE) != BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write temporary file \"%s\": %m", path)));

	INSTR_TIME_SET_CURRENT(start);
	for (int64 i = 0; i < iterations; i++)
	{
		ssize_t		nread;

		nread = FileRead(file, buffer, BLCKSZ, 0,
						 WAIT_EVENT_DATA_FILE_READ);
		if (nread != BLCKSZ)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read temporary file \"%s\": %m", path)));
		sink += (uint64) nread;
	}
	INSTR_TIME_SET_CURRENT(end);

	sink += (unsigned char) buffer[0];
	FileClose(file);
	if (unlink(path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove temporary file \"%s\": %m", path)));
	pfree(buffer);

	wait_primitive_sink ^= sink;
	PG_RETURN_FLOAT8(nanoseconds_per_iteration(start, end, iterations));
}

Datum
test_wait_primitive_usleep0(PG_FUNCTION_ARGS)
{
	int64		iterations = get_iterations(fcinfo);
	instr_time start;
	instr_time end;

	INSTR_TIME_SET_CURRENT(start);
	for (int64 i = 0; i < iterations; i++)
	{
		pgstat_report_wait_start(WAIT_EVENT_PG_SLEEP);
		pg_usleep(0);
		pgstat_report_wait_end();
	}
	INSTR_TIME_SET_CURRENT(end);

	wait_primitive_sink ^= (uint64) iterations;
	PG_RETURN_FLOAT8(nanoseconds_per_iteration(start, end, iterations));
}

Datum
test_wait_primitive_report_only(PG_FUNCTION_ARGS)
{
	int64		iterations = get_iterations(fcinfo);
	instr_time start;
	instr_time end;

	INSTR_TIME_SET_CURRENT(start);
	for (int64 i = 0; i < iterations; i++)
	{
		pgstat_report_wait_start(WAIT_EVENT_PG_SLEEP);
		pgstat_report_wait_end();
	}
	INSTR_TIME_SET_CURRENT(end);

	wait_primitive_sink ^= (uint64) iterations;
	PG_RETURN_FLOAT8(nanoseconds_per_iteration(start, end, iterations));
}
