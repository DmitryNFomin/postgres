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
 * Copyright (c) 2026, PostgreSQL Global Development Group
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
#include "funcapi.h"
#include "utils/guc_hooks.h"
#include "utils/wait_event_timing.h"

Datum		pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS);
Datum		pg_stat_get_wait_event_trace(PG_FUNCTION_ARGS);
Datum		pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS);

Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_stat_get_wait_event_trace(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);
	PG_RETURN_VOID();
}

Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("wait_event_timing is not supported by this build"),
			 errhint("Compile PostgreSQL with --enable-wait-event-timing.")));
	PG_RETURN_VOID();
}

/*
 * Extern variables referenced by backend_status.c unconditionally.
 * In timing builds these are defined after the #else.
 */
/* GUC check hooks: reject 'on' in unsupported builds */
bool
check_wait_event_timing(bool *newval, void **extra, GucSource source)
{
	if (*newval)
	{
		if (source < PGC_S_INTERACTIVE)
		{
			/* Config file, env, command line: force off so server can start */
			*newval = false;
			return true;
		}
		GUC_check_errdetail("This build does not support wait event timing.");
		GUC_check_errhint("Compile PostgreSQL with --enable-wait-event-timing.");
		return false;
	}
	return true;
}

bool
check_wait_event_trace(bool *newval, void **extra, GucSource source)
{
	if (*newval)
	{
		if (source < PGC_S_INTERACTIVE)
		{
			*newval = false;
			return true;
		}
		GUC_check_errdetail("This build does not support wait event tracing.");
		GUC_check_errhint("Compile PostgreSQL with --enable-wait-event-timing.");
		return false;
	}
	return true;
}

/* Stub GUC assign hook */
void
assign_wait_event_trace(bool newval, void *extra)
{
	/* no-op in non-timing builds */
}

/* Stub shmem functions called from ipci.c */
Size
WaitEventTimingShmemSize(void)
{
	return 0;
}

void
WaitEventTimingShmemInit(void)
{
}

Size
WaitEventTraceControlShmemSize(void)
{
	return 0;
}

void
WaitEventTraceControlShmemInit(void)
{
}

void
pgstat_set_wait_event_timing_storage(int procNumber)
{
}

void
pgstat_reset_wait_event_timing_storage(void)
{
}

#else							/* USE_WAIT_EVENT_TIMING */

#include "catalog/pg_authid.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "catalog/pg_type_d.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"
#include "utils/wait_event_timing.h"

#define NUM_WAIT_EVENT_TIMING_SLOTS  (MaxBackends + NUM_AUXILIARY_PROCS)

#define HAS_PGSTAT_PERMISSIONS(role) \
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

/* Pointer to this backend's timing state */
WaitEventTimingState *my_wait_event_timing = NULL;

/* Pointer to this backend's trace ring buffer */
WaitEventTraceState *my_wait_event_trace = NULL;

/* Shared memory base pointers */
static WaitEventTimingState *WaitEventTimingArray = NULL;

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
	return mul_size(NUM_WAIT_EVENT_TIMING_SLOTS, sizeof(WaitEventTimingState));
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
 * Report the shared memory space needed for trace ring buffer control.
 * Only a small control struct is in fixed shmem; the actual ring buffers
 * are allocated lazily via DSA.
 */
Size
WaitEventTraceControlShmemSize(void)
{
	return add_size(offsetof(WaitEventTraceControl, trace_ptrs),
					mul_size(NUM_WAIT_EVENT_TIMING_SLOTS, sizeof(dsa_pointer)));
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
		for (i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
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

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
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

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
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
 * GUC check hooks -- in timing builds, all values are accepted.
 */
bool
check_wait_event_timing(bool *newval, void **extra, GucSource source)
{
	return true;
}

bool
check_wait_event_trace(bool *newval, void **extra, GucSource source)
{
	return true;
}

/*
 * GUC assign hook for wait_event_trace.
 * Warns if wait_event_timing is off, since trace has no effect without it.
 * Lazily allocates the DSA-backed trace ring buffer on first enable.
 */
void
assign_wait_event_trace(bool newval, void *extra)
{
	if (newval && !wait_event_timing)
		ereport(WARNING,
				(errmsg("wait_event_trace has no effect unless wait_event_timing is enabled"),
				 errhint("Ask a superuser to SET wait_event_timing = on.")));

	if (newval && my_wait_event_trace == NULL && my_trace_proc_number >= 0)
		wait_event_trace_attach(my_trace_proc_number);
}

/*
 * Point my_wait_event_timing at this backend's slot.
 * Called from InitProcess() after the backend has a valid procNumber.
 *
 * procNumber is the PGPROC array index (from GetNumberFromPGProc).
 * Covers both regular backends (procNumber < MaxBackends) and auxiliary
 * processes (bgwriter, checkpointer, walwriter, etc.).
 */
void
pgstat_set_wait_event_timing_storage(int procNumber)
{
	if (WaitEventTimingArray == NULL)
		return;

	if (procNumber < 0 || procNumber >= NUM_WAIT_EVENT_TIMING_SLOTS)
	{
		my_wait_event_timing = NULL;
		return;
	}

	my_wait_event_timing = &WaitEventTimingArray[procNumber];

	/* Zero the state for this new backend session */
	memset(my_wait_event_timing, 0, sizeof(WaitEventTimingState));

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

	/* Trace ring buffer: cleanup via before_shmem_exit callback (Fix #1) */
	if (my_trace_proc_number >= 0)
		wait_event_trace_detach(my_trace_proc_number);

	my_wait_event_timing = NULL;
	my_wait_event_trace = NULL;
	my_trace_proc_number = -1;
}

/*
 * Out-of-line body for pgstat_report_wait_end() timing path.
 * Called when wait_event_timing GUC is on and my_wait_event_timing is set.
 * Computes wait duration, accumulates per-event stats, and optionally
 * writes to the trace ring buffer.
 */
void
pgstat_report_wait_end_timing(void)
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

		if (unlikely(duration_ns < 0))
			duration_ns = 0;

		idx = wait_event_timing_index(event);

		{
			WaitEventTimingEntry *entry = NULL;

			if (idx == WAIT_EVENT_TIMING_IDX_LWLOCK)
				entry = lwlock_timing_lookup(
					&my_wait_event_timing->lwlock_hash,
					event & 0xFFFF);
			else if (likely(idx >= 0))
				entry = &my_wait_event_timing->events[idx];

			if (likely(entry != NULL))
			{
				entry->count++;
				entry->total_ns += duration_ns;
				if (duration_ns > entry->max_ns)
					entry->max_ns = duration_ns;
				entry->histogram[wait_event_timing_bucket(duration_ns)]++;
			}
			else if (idx == WAIT_EVENT_TIMING_IDX_LWLOCK)
				my_wait_event_timing->lwlock_overflow_count++;
		}

		/* 10046-style per-session trace ring buffer (DSA-backed) */
		if (unlikely(wait_event_trace && my_wait_event_trace != NULL))
		{
			uint64	pos = pg_atomic_fetch_add_u64(&my_wait_event_trace->write_pos, 1);
			WaitEventTraceRecord *rec =
				&my_wait_event_trace->records[pos & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
			uint32	seq = (uint32)(pos * 2 + 1);

			rec->seq = seq;
			pg_write_barrier();

			rec->record_type = TRACE_WAIT_EVENT;
			rec->timestamp_ns = INSTR_TIME_GET_NANOSEC(now);
			rec->data.wait.event = event;
			rec->data.wait.pad2 = 0;
			rec->data.wait.duration_ns = duration_ns;

			pg_write_barrier();
			rec->seq = seq + 1;
		}

		INSTR_TIME_SET_ZERO(my_wait_event_timing->wait_start);
	}
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

	for (backend_idx = 0; backend_idx < NUM_WAIT_EVENT_TIMING_SLOTS; backend_idx++)
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

		/* Emit rows from the flat array (all classes except LWLock) */
		for (i = 0; i < WAIT_EVENT_TIMING_DENSE_CLASSES; i++)
		{
			int		base = wait_event_class_offset[i];
			int		nevents = wait_event_class_nevents[i];
			uint32	classId = wait_event_dense_to_classid[i];
			int		j;

			for (j = 0; j < nevents; j++)
			{
				WaitEventTimingEntry *entry = &state->events[base + j];
				Datum		values[10];
				bool		nulls[10];
				uint32		wait_event_info;
				const char *event_type;
				const char *event_name;
				int			bucket;

				if (entry->count == 0)
					continue;

				/* Reconstruct wait_event_info from class and event ID */
				wait_event_info = ((uint32) classId << 24) | j;

				event_type = pgstat_get_wait_event_type(wait_event_info);
				event_name = pgstat_get_wait_event(wait_event_info);

				if (event_type == NULL || event_name == NULL)
					continue;

				memset(nulls, 0, sizeof(nulls));

				values[0] = Int32GetDatum(beentry->st_procpid);
				values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
				values[2] = Int32GetDatum(backend_idx + 1);
				values[3] = CStringGetTextDatum(event_type);
				values[4] = CStringGetTextDatum(event_name);
				values[5] = Int64GetDatum(entry->count);
				values[6] = Float8GetDatum((double) entry->total_ns / 1000000.0);
				values[7] = Float8GetDatum(entry->count > 0
										   ? (double) entry->total_ns / entry->count / 1000.0
										   : 0.0);
				values[8] = Float8GetDatum((double) entry->max_ns / 1000.0);

				{
					Datum	elems[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];

					for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
						elems[bucket] = Int64GetDatum(entry->histogram[bucket]);

					values[9] = PointerGetDatum(
						construct_array_builtin(elems,
												WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS,
												INT8OID));
				}

				tuplestore_putvalues(rsinfo->setResult,
									rsinfo->setDesc,
									values, nulls);
			}
		}

		/* Emit rows from the LWLock hash table */
		for (i = 0; i < LWLOCK_TIMING_HASH_SIZE; i++)
		{
			LWLockTimingHashEntry *he = &state->lwlock_hash.entries[i];
			WaitEventTimingEntry *entry;
			Datum		values[10];
			bool		nulls[10];
			uint32		wait_event_info;
			const char *event_type;
			const char *event_name;
			int			bucket;

			if (he->tranche_id == 0)
				continue;

			entry = &state->lwlock_hash.lwlock_events[he->dense_idx];
			if (entry->count == 0)
				continue;

			wait_event_info = PG_WAIT_LWLOCK | he->tranche_id;

			event_type = pgstat_get_wait_event_type(wait_event_info);
			event_name = pgstat_get_wait_event(wait_event_info);

			if (event_type == NULL || event_name == NULL)
				continue;

			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(beentry->st_procpid);
			values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
			values[2] = Int32GetDatum(backend_idx + 1);
			values[3] = CStringGetTextDatum(event_type);
			values[4] = CStringGetTextDatum(event_name);
			values[5] = Int64GetDatum(entry->count);
			values[6] = Float8GetDatum((double) entry->total_ns / 1000000.0);
			values[7] = Float8GetDatum(entry->count > 0
									   ? (double) entry->total_ns / entry->count / 1000.0
									   : 0.0);
			values[8] = Float8GetDatum((double) entry->max_ns / 1000.0);

			{
				Datum	elems[WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS];

				for (bucket = 0; bucket < WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS; bucket++)
					elems[bucket] = Int64GetDatum(entry->histogram[bucket]);

				values[9] = PointerGetDatum(
					construct_array_builtin(elems,
											WAIT_EVENT_TIMING_HISTOGRAM_BUCKETS,
											INT8OID));
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
/*
 * Local hash entry for trace-ring-based query attribution.
 * Keyed by (query_id, event), accumulated at read time.
 */
typedef struct QueryAttrKey
{
	int64		query_id;
	uint32		event;
	uint8		phase;			/* 0 = plan, 1 = exec */
} QueryAttrKey;

typedef struct QueryAttrEntry
{
	QueryAttrKey key;
	int64		count;
	int64		total_ns;
	char		status;			/* for simplehash */
} QueryAttrEntry;

#define SH_PREFIX		queryattr
#define SH_ELEMENT_TYPE	QueryAttrEntry
#define SH_KEY_TYPE		QueryAttrKey
#define SH_KEY			key
#define SH_HASH_KEY(tb, key) \
	((uint32)((uint64)(key).query_id * 0x9e3779b97f4a7c15ULL) ^ (key).event ^ ((uint32)(key).phase << 16))
#define SH_EQUAL(tb, a, b) \
	((a).query_id == (b).query_id && (a).event == (b).event && (a).phase == (b).phase)
#define SH_SCOPE		static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

Datum
pg_stat_get_wait_event_timing_by_query(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			backend_idx;

	InitMaterializedSRF(fcinfo, 0);

	if (WaitEventTraceCtl == NULL)
		PG_RETURN_VOID();

	for (backend_idx = 0; backend_idx < NUM_WAIT_EVENT_TIMING_SLOTS; backend_idx++)
	{
		WaitEventTraceState *ts;
		PgBackendStatus *beentry;
		queryattr_hash *ht;
		uint64		write_pos;
		uint64		read_start;
		uint64		i;
		int64		current_qid = 0;
		uint8		current_phase = 0;	/* 0=plan, 1=exec */
		queryattr_iterator iter;
		QueryAttrEntry *entry;

		/* Skip dead backend slots and check permissions */
		beentry = pgstat_get_beentry_by_proc_number(backend_idx);
		if (beentry == NULL)
			continue;
		if (!HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
			continue;

		/* Skip backends without trace ring */
		if (!DsaPointerIsValid(WaitEventTraceCtl->trace_ptrs[backend_idx]))
			continue;

		wait_event_trace_ensure_dsa();
		ts = dsa_get_address(trace_dsa,
							 WaitEventTraceCtl->trace_ptrs[backend_idx]);

		write_pos = pg_atomic_read_u64(&ts->write_pos);
		if (write_pos == 0)
			continue;

		read_start = (write_pos > WAIT_EVENT_TRACE_RING_SIZE)
			? write_pos - WAIT_EVENT_TRACE_RING_SIZE : 0;

		/* Build local hash from trace ring */
		ht = queryattr_create(CurrentMemoryContext, 256, NULL);

		for (i = read_start; i < write_pos; i++)
		{
			WaitEventTraceRecord *rec =
				&ts->records[i & (WAIT_EVENT_TRACE_RING_SIZE - 1)];
			uint32		seq_before, seq_after;
			uint8		rtype;
			int64		ts_ns;
			uint32		evt;
			int64		dur_ns;
			int64		qid;
			bool		found;

			seq_before = rec->seq;
			pg_read_barrier();
			if (seq_before & 1)
				continue;

			rtype = rec->record_type;
			ts_ns = rec->timestamp_ns;
			if (rtype == TRACE_WAIT_EVENT)
			{
				evt = rec->data.wait.event;
				dur_ns = rec->data.wait.duration_ns;
			}
			else
			{
				qid = rec->data.query.query_id;
			}

			pg_read_barrier();
			seq_after = rec->seq;
			if (seq_before != seq_after)
				continue;

			if (rtype == TRACE_QUERY_START)
			{
				current_qid = qid;
				current_phase = 0;	/* planning */
			}
			else if (rtype == TRACE_EXEC_START)
			{
				current_phase = 1;	/* execution */
			}
			else if (rtype == TRACE_QUERY_END)
			{
				current_qid = 0;
				current_phase = 0;
			}
			else if (rtype == TRACE_WAIT_EVENT && current_qid != 0 && evt != 0)
			{
				QueryAttrKey k;
				QueryAttrEntry *e;

				k.query_id = current_qid;
				k.event = evt;
				k.phase = current_phase;
				e = queryattr_insert(ht, k, &found);
				if (!found)
				{
					e->count = 0;
					e->total_ns = 0;
				}
				e->count++;
				e->total_ns += dur_ns;
			}
		}

		/* Emit rows from local hash */
		queryattr_start_iterate(ht, &iter);
		while ((entry = queryattr_iterate(ht, &iter)) != NULL)
		{
			Datum		values[9];
			bool		nulls[9];
			const char *event_type;
			const char *event_name;

			event_type = pgstat_get_wait_event_type(entry->key.event);
			event_name = pgstat_get_wait_event(entry->key.event);
			if (event_type == NULL || event_name == NULL)
				continue;

			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(beentry->st_procpid);
			values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
			values[2] = Int32GetDatum(backend_idx + 1);
			values[3] = Int64GetDatum(entry->key.query_id);
			values[4] = CStringGetTextDatum(entry->key.phase == 0 ? "plan" : "exec");
			values[5] = CStringGetTextDatum(event_type);
			values[6] = CStringGetTextDatum(event_name);
			values[7] = Int64GetDatum(entry->count);
			values[8] = Float8GetDatum((double) entry->total_ns / 1000000.0);

			tuplestore_putvalues(rsinfo->setResult,
								rsinfo->setDesc,
								values, nulls);
		}

		queryattr_destroy(ht);
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

	if (backend_idx < 0 || backend_idx >= NUM_WAIT_EVENT_TIMING_SLOTS)
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
		uint32		seq_before;
		uint32		seq_after;
		uint8		rtype;
		int64		timestamp_ns;
		uint32		event_info;
		int64		duration_ns;
		int64		query_id;

		/* Seqlock read */
		seq_before = rec->seq;
		pg_read_barrier();

		if (seq_before & 1)
			continue;

		rtype = rec->record_type;
		timestamp_ns = rec->timestamp_ns;

		if (rtype == TRACE_WAIT_EVENT)
		{
			event_info = rec->data.wait.event;
			duration_ns = rec->data.wait.duration_ns;
			query_id = 0;
		}
		else if (rtype == TRACE_QUERY_START || rtype == TRACE_QUERY_END ||
				 rtype == TRACE_EXEC_START)
		{
			event_info = 0;
			duration_ns = 0;
			query_id = rec->data.query.query_id;
		}
		else
		{
			pg_read_barrier();
			continue;
		}

		pg_read_barrier();
		seq_after = rec->seq;

		if (seq_before != seq_after)
			continue;

		/* Skip empty wait events */
		if (rtype == TRACE_WAIT_EVENT && event_info == 0)
			continue;

		if (rtype == TRACE_WAIT_EVENT)
		{
			event_type = pgstat_get_wait_event_type(event_info);
			event_name = pgstat_get_wait_event(event_info);
		}
		else if (rtype == TRACE_QUERY_START)
		{
			event_type = "Query";
			event_name = "QueryStart";
		}
		else if (rtype == TRACE_EXEC_START)
		{
			event_type = "Query";
			event_name = "ExecStart";
		}
		else
		{
			event_type = "Query";
			event_name = "QueryEnd";
		}

		if (event_type == NULL || event_name == NULL)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum((int64) i);
		values[1] = Int64GetDatum(timestamp_ns);
		values[2] = CStringGetTextDatum(event_type);
		values[3] = CStringGetTextDatum(event_name);
		values[4] = Float8GetDatum((double) duration_ns / 1000.0);
		values[5] = Int64GetDatum(query_id);

		tuplestore_putvalues(rsinfo->setResult,
							rsinfo->setDesc,
							values, nulls);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_reset_wait_event_timing(backend_id int4)
 *
 * Resets wait event timing counters.
 *   backend_id = NULL or 0: reset own backend
 *   backend_id > 0: reset specific backend (superuser only)
 *   backend_id = -1: reset ALL backends (superuser only)
 */
Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	int			target;

	if (PG_ARGISNULL(0) || PG_GETARG_INT32(0) == 0)
	{
		/* Reset own backend -- no privilege check needed */
		if (my_wait_event_timing != NULL)
		{
			memset(my_wait_event_timing->events, 0,
				   sizeof(my_wait_event_timing->events));
			memset(&my_wait_event_timing->lwlock_hash, 0,
				   sizeof(LWLockTimingHash));
			my_wait_event_timing->reset_count++;
		}
	}
	else
	{
		target = PG_GETARG_INT32(0);

		/* Resetting other backends requires superuser */
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be a superuser to reset other backends' wait event timing")));

		if (target == -1)
		{
			/* Reset all backends */
			for (int i = 0; i < NUM_WAIT_EVENT_TIMING_SLOTS; i++)
			{
				memset(WaitEventTimingArray[i].events, 0,
					   sizeof(WaitEventTimingArray[i].events));
				memset(&WaitEventTimingArray[i].lwlock_hash, 0,
					   sizeof(LWLockTimingHash));
				WaitEventTimingArray[i].reset_count++;
			}
		}
		else
		{
			int			idx = target - 1;	/* 1-based to 0-based */

			if (idx < 0 || idx >= NUM_WAIT_EVENT_TIMING_SLOTS)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid backend_id: %d", target)));

			memset(WaitEventTimingArray[idx].events, 0,
				   sizeof(WaitEventTimingArray[idx].events));
			memset(&WaitEventTimingArray[idx].lwlock_hash, 0,
				   sizeof(LWLockTimingHash));
			WaitEventTimingArray[idx].reset_count++;
		}
	}

	PG_RETURN_VOID();
}

#endif							/* USE_WAIT_EVENT_TIMING */
