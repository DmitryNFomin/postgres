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

/*
 * GUC variables — always defined so the GUC system works even when
 * compiled without --enable-wait-event-timing.  Setting them to 'on'
 * without the compile flag is harmless (no-op).
 */
bool		wait_event_timing = false;
bool		wait_event_trace = false;

#ifndef USE_WAIT_EVENT_TIMING

/*
 * Stub SQL functions when compiled without --enable-wait-event-timing.
 * These are referenced by pg_proc.dat and must exist as symbols.
 */
#include "fmgr.h"

Datum		pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_trace(PG_FUNCTION_ARGS);

Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("wait_event_timing is not supported by this build"),
			 errhint("Compile PostgreSQL with --enable-wait-event-timing.")));
	PG_RETURN_VOID();
}

Datum
pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("wait_event_timing is not supported by this build"),
			 errhint("Compile PostgreSQL with --enable-wait-event-timing.")));
	PG_RETURN_VOID();
}

Datum
pg_stat_get_wait_event_trace(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("wait_event_timing is not supported by this build"),
			 errhint("Compile PostgreSQL with --enable-wait-event-timing.")));
	PG_RETURN_VOID();
}

#else							/* USE_WAIT_EVENT_TIMING */

#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"
#include "utils/wait_event_timing.h"

#define HAS_PGSTAT_PERMISSIONS(role) \
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

/* Pointer to this backend's timing state */
WaitEventTimingState *my_wait_event_timing = NULL;

/* Pointer to this backend's query attribution hash */
WaitEventQueryState *my_wait_event_query = NULL;

/* Pointer to this backend's trace ring buffer */
WaitEventTraceState *my_wait_event_trace = NULL;

/* Pointer to current backend's query_id in PgBackendStatus */
volatile int64 *my_wait_event_query_id_ptr = NULL;

/* Shared memory base pointers */
static WaitEventTimingState *WaitEventTimingArray = NULL;
static WaitEventQueryState *WaitEventQueryArray = NULL;

/* DSA-based trace ring buffer control */
static WaitEventTraceControl *WaitEventTraceCtl = NULL;
static dsa_area *trace_dsa = NULL;
static int	my_trace_proc_number = -1;

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
 * Report the shared memory space needed for query attribution.
 */
Size
WaitEventQueryShmemSize(void)
{
	return mul_size(MaxBackends, sizeof(WaitEventQueryState));
}

/*
 * Initialize shared memory for query attribution.
 */
void
WaitEventQueryShmemInit(void)
{
	bool		found;
	Size		size;

	size = WaitEventQueryShmemSize();

	WaitEventQueryArray = (WaitEventQueryState *)
		ShmemInitStruct("WaitEventQueryArray", size, &found);

	if (!found)
		memset(WaitEventQueryArray, 0, size);
}

/*
 * Report the shared memory space needed for trace ring buffer control.
 * Only a small control struct is in fixed shmem; the actual ring buffers
 * are allocated lazily via DSA.
 */
Size
WaitEventTraceControlShmemSize(void)
{
	return add_size(offsetof(WaitEventTraceControl, trace_ptrs),
					mul_size(MaxBackends, sizeof(dsa_pointer)));
}

/*
 * Initialize shared memory for trace ring buffer control.
 */
void
WaitEventTraceControlShmemInit(void)
{
	bool		found;
	Size		size;

	size = WaitEventTraceControlShmemSize();

	WaitEventTraceCtl = (WaitEventTraceControl *)
		ShmemInitStruct("WaitEventTraceControl", size, &found);

	if (!found)
	{
		int		i;

		WaitEventTraceCtl->trace_dsa_handle = DSA_HANDLE_INVALID;
		LWLockInitialize(&WaitEventTraceCtl->lock,
						 LWTRANCHE_WAIT_EVENT_TRACE_DSA);
		for (i = 0; i < MaxBackends; i++)
			WaitEventTraceCtl->trace_ptrs[i] = InvalidDsaPointer;
	}
}

/*
 * Ensure the shared DSA for trace ring buffers exists and is attached.
 * Creates it on first call (any backend), attaches on subsequent calls.
 * Must be called from a backend context (not postmaster).
 */
void
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
 * Free trace ring buffer for this backend.
 *
 * Must be called BEFORE dsm_backend_shutdown() detaches the DSA.
 * Registered as a before_shmem_exit callback.
 */
static void
wait_event_trace_before_shmem_exit(int code, Datum arg)
{
	int		procNumber = DatumGetInt32(arg);

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= MaxBackends)
		return;

	if (DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[procNumber]) &&
		trace_dsa != NULL)
	{
		dsa_free(trace_dsa, WaitEventTraceCtl->trace_ptrs[procNumber]);
		WaitEventTraceCtl->trace_ptrs[procNumber] = InvalidDsaPointer;
	}

	my_wait_event_trace = NULL;
}

/*
 * Allocate a trace ring buffer for this backend via DSA.
 * Called when wait_event_trace is turned on.
 */
void
wait_event_trace_attach(int procNumber)
{
	dsa_pointer p;
	WaitEventTraceState *ts;

	if (WaitEventTraceCtl == NULL)
		return;

	if (procNumber < 0 || procNumber >= MaxBackends)
		return;

	/* Already have a ring buffer? */
	if (DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[procNumber]))
	{
		wait_event_trace_ensure_dsa();
		my_wait_event_trace = dsa_get_address(trace_dsa,
											  WaitEventTraceCtl->trace_ptrs[procNumber]);
		my_trace_proc_number = procNumber;
		return;
	}

	wait_event_trace_ensure_dsa();

	p = dsa_allocate_extended(trace_dsa, sizeof(WaitEventTraceState),
							  DSA_ALLOC_ZERO);
	ts = dsa_get_address(trace_dsa, p);
	pg_atomic_init_u64(&ts->write_pos, 0);

	WaitEventTraceCtl->trace_ptrs[procNumber] = p;
	my_wait_event_trace = ts;
	my_trace_proc_number = procNumber;

	/*
	 * Register cleanup to run BEFORE dsm_backend_shutdown() detaches the
	 * DSA.  The before_shmem_exit callbacks run in LIFO order before DSM
	 * detach, so dsa_free() is safe at that point.
	 *
	 * This branch executes at most once per backend lifetime: subsequent
	 * SET wait_event_trace = on takes the reattach fast path above because
	 * trace_ptrs[procNumber] remains valid until exit.
	 */
	before_shmem_exit(wait_event_trace_before_shmem_exit,
					  Int32GetDatum(procNumber));
}

/*
 * Free trace ring buffer for this backend on exit.
 */
void
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
 * GUC assign hook for wait_event_trace.
 * Lazily allocates the DSA-backed trace ring buffer on first enable.
 */
void
assign_wait_event_trace(bool newval, void *extra)
{
	if (newval && my_wait_event_trace == NULL && my_trace_proc_number >= 0)
		wait_event_trace_attach(my_trace_proc_number);
}

/*
 * Point my_wait_event_timing at this backend's slot.
 * Called from InitProcess() after the backend has a valid procNumber.
 *
 * procNumber is the PGPROC array index (from GetNumberFromPGProc).
 * Auxiliary processes (startup, bgwriter, etc.) have procNumbers beyond
 * MaxBackends — we silently skip timing for them.
 */
void
pgstat_set_wait_event_timing_storage(int procNumber)
{
	if (WaitEventTimingArray == NULL)
		return;

	/* Auxiliary processes have procNumbers >= MaxBackends; skip them */
	if (procNumber < 0 || procNumber >= MaxBackends)
	{
		my_wait_event_timing = NULL;
		return;
	}

	my_wait_event_timing = &WaitEventTimingArray[procNumber];

	/* Zero the state for this new backend session */
	memset(my_wait_event_timing, 0, sizeof(WaitEventTimingState));

	/* Set up query attribution hash (same index) */
	if (WaitEventQueryArray != NULL)
	{
		my_wait_event_query = &WaitEventQueryArray[procNumber];
		memset(my_wait_event_query, 0, sizeof(WaitEventQueryState));
	}

	/*
	 * Trace ring buffer is allocated lazily via DSA when wait_event_trace
	 * is turned on.  Save procNumber for later use by trace_attach/detach.
	 */
	my_trace_proc_number = procNumber;
	my_wait_event_trace = NULL;
}

/*
 * Detach from timing state on backend exit.
 */
void
pgstat_reset_wait_event_timing_storage(void)
{
	/*
	 * Zero shared memory so stale data is not visible even at shmem level.
	 * Reader-side filtering (Fix #2) skips dead backends via beentry==NULL,
	 * but zeroing here handles the crash recovery case and ensures clean
	 * state for the next backend on this slot.
	 */
	if (my_wait_event_timing != NULL)
		memset(my_wait_event_timing, 0, sizeof(WaitEventTimingState));

	if (my_wait_event_query != NULL)
		memset(my_wait_event_query, 0, sizeof(WaitEventQueryState));

	/* Trace ring buffer: cleanup via before_shmem_exit callback (Fix #1) */
	if (my_trace_proc_number >= 0)
		wait_event_trace_detach(my_trace_proc_number);

	my_wait_event_timing = NULL;
	my_wait_event_query = NULL;
	my_wait_event_trace = NULL;
	my_wait_event_query_id_ptr = NULL;
	my_trace_proc_number = -1;
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
		PgBackendStatus *beentry;
		int			i;

		/* Skip dead backend slots and check permissions */
		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			continue;
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			continue;

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

/*
 * SQL function: pg_stat_get_wait_event_timing_by_query(OUT ...)
 *
 * Returns one row per (backend_id, query_id, wait_event) with non-zero counts.
 */
Datum
pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	if (WaitEventQueryArray == NULL)
		PG_RETURN_VOID();

	for (backend_idx = 0; backend_idx < MaxBackends; backend_idx++)
	{
		WaitEventQueryState *qs = &WaitEventQueryArray[backend_idx];
		PgBackendStatus *beentry;
		int			i;

		/* Skip dead backend slots and check permissions */
		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			continue;
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			continue;

		for (i = 0; i < WAIT_EVENT_QUERY_HASH_SIZE; i++)
		{
			WaitEventQueryEntry *entry = &qs->entries[i];
			Datum		values[6];
			bool		nulls[6];
			uint32		wait_event_info;
			const char *event_type;
			const char *event_name;

			if (entry->query_id == 0)
				continue;

			/* Reconstruct wait_event_info from flat index */
			wait_event_info =
				((entry->event_idx / WAIT_EVENT_TIMING_EVENTS_PER_CLASS) << 24) |
				(entry->event_idx % WAIT_EVENT_TIMING_EVENTS_PER_CLASS);

			event_type = pgstat_get_wait_event_type(wait_event_info);
			event_name = pgstat_get_wait_event(wait_event_info);

			if (event_type == NULL || event_name == NULL)
				continue;

			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(backend_idx + 1);
			values[1] = Int64GetDatum(entry->query_id);
			values[2] = CStringGetTextDatum(event_type);
			values[3] = CStringGetTextDatum(event_name);
			values[4] = Int64GetDatum(entry->count);
			values[5] = Float8GetDatum((double) entry->total_ns / 1000000.0);

			tuplestore_putvalues(rsinfo->setResult,
								rsinfo->setDesc,
								values, nulls);
		}
	}

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_get_wait_event_trace(backend_id int4)
 *
 * Returns trace records from a backend's ring buffer in chronological order.
 * Pass NULL or 0 for own backend.
 */
Datum
pg_stat_get_wait_event_trace(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;
	WaitEventTraceState *ts;
	uint64		write_pos;
	uint64		read_start;
	uint64		i;

	InitMaterializedSRF(fcinfo, 0);

	if (WaitEventTraceCtl == NULL)
		PG_RETURN_VOID();

	/* Determine which backend to read */
	if (PG_ARGISNULL(0) || PG_GETARG_INT32(0) <= 0)
	{
		/* Own backend */
		if (my_wait_event_trace == NULL)
			PG_RETURN_VOID();
		backend_idx = my_trace_proc_number;
	}
	else
	{
		backend_idx = PG_GETARG_INT32(0) - 1;	/* 1-based to 0-based */
	}

	if (backend_idx < 0 || backend_idx >= MaxBackends)
		PG_RETURN_VOID();

	/* Permission check: only own backend or privileged roles */
	if (backend_idx != my_trace_proc_number)
	{
		PgBackendStatus *beentry;

		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			PG_RETURN_VOID();
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to read wait event trace for backend %d",
							backend_idx + 1)));
	}

	if (!DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[backend_idx]))
		PG_RETURN_VOID();

	/* Attach to DSA if needed and resolve the pointer */
	wait_event_trace_ensure_dsa();
	ts = dsa_get_address(trace_dsa,
						 WaitEventTraceCtl->trace_ptrs[backend_idx]);

	write_pos = pg_atomic_read_u64(&ts->write_pos);

	if (write_pos == 0)
		PG_RETURN_VOID();

	/* Read from oldest available to newest */
	read_start = (write_pos > WAIT_EVENT_TRACE_RING_SIZE)
		? write_pos - WAIT_EVENT_TRACE_RING_SIZE : 0;

	for (i = read_start; i < write_pos; i++)
	{
		WaitEventTraceRecord *rec =
			&ts->records[i & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
		Datum		values[6];
		bool		nulls[6];
		const char *event_type;
		const char *event_name;

		if (rec->event == 0)
			continue;

		event_type = pgstat_get_wait_event_type(rec->event);
		event_name = pgstat_get_wait_event(rec->event);
		if (event_type == NULL || event_name == NULL)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum((int64) i);		/* seq */
		values[1] = Int64GetDatum(rec->timestamp_ns);
		values[2] = CStringGetTextDatum(event_type);
		values[3] = CStringGetTextDatum(event_name);
		values[4] = Float8GetDatum((double) rec->duration_ns / 1000.0);
		values[5] = Int64GetDatum(rec->query_id);

		tuplestore_putvalues(rsinfo->setResult,
							rsinfo->setDesc,
							values, nulls);
	}

	PG_RETURN_VOID();
}

#endif							/* USE_WAIT_EVENT_TIMING */
