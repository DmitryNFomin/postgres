/*-------------------------------------------------------------------------
 *
 * pg_wait_event_tracing.c
 *	  Statistics-level wait-event collector.
 *
 * The recorder is the peer-review package's collector, ported onto the
 * begin/end wait-event hooks and renamed.  Each collecting backend owns one
 * sparse DSA slot, addressed through a small, always-resident control
 * segment (GetNamedDSMSegment()); the roughly 200 KiB-per-backend timing
 * payload itself lives in a DSA area (GetNamedDSA()) and is allocated only
 * for a backend that actually enables capture.  Hook callbacks only touch
 * preallocated backend-local pointers: allocation, locking, and
 * error-capable work happen from parse/executor safe points, never from the
 * begin/end hooks themselves.
 *
 * This file carries the statistics level only.  The trace level (per-backend
 * ring buffer, query markers, trace SRFs) is a separate patch; the slot
 * layout below reserves the fields that level will need (trace_ptr,
 * trace_state) so that addition does not reshape the control segment.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "parser/analyze.h"
#include "port/pg_bitutils.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procnumber.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/dsa.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/tuplestore.h"
#include "utils/wait_classes.h"
#include "utils/wait_event.h"

#include "pg_wait_event_tracing_data.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_wait_event_tracing",
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(pg_stat_get_wait_event_timing);
PG_FUNCTION_INFO_V1(pg_stat_get_wait_event_timing_overflow);
PG_FUNCTION_INFO_V1(pg_stat_reset_wait_event_timing);
PG_FUNCTION_INFO_V1(pg_stat_reset_wait_event_timing_all);

PGDLLEXPORT void _PG_init(void);

#define PWET_CONTROL_NAME "pg_wait_event_tracing"
#define PWET_STATS_DSA_NAME "pg_wait_event_tracing_stats"
#define PWET_NUM_SLOTS (MaxBackends + NUM_AUXILIARY_PROCS)
#define PWET_HISTOGRAM_BUCKETS 32
#define PWET_IDX_LWLOCK (-2)
#define PWET_LWLOCK_EMPTY ((uint16) 0xFFFF)
#define PWET_WAIT_EVENT_CLASS_MASK 0xFF000000U
#define PWET_WAIT_EVENT_ID_MASK 0x0000FFFFU
#define PWET_LWLOCK_PROBE_LIMIT 8
#define PWET_HAS_STATS_PRIVS(role) \
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

/*
 * Reserved trace_state values.  The trace-level patch adds ACTIVE and
 * ORPHANED; this module only ever produces FREE.
 */
#define PWET_TRACE_FREE 0

typedef enum PwetCaptureLevel
{
	PWET_CAPTURE_OFF = 0,
	PWET_CAPTURE_STATS,
} PwetCaptureLevel;

typedef struct PwetTimingEntry
{
	int64		count;
	int64		total_ns;
	int64		max_ns;
	int64		histogram[PWET_HISTOGRAM_BUCKETS];
} PwetTimingEntry;

typedef struct PwetLWLockHashEntry
{
	uint16		tranche_id;
	uint16		dense_idx;
} PwetLWLockHashEntry;

typedef struct PwetLWLockHash
{
	int			num_used;
	int			hash_size;
	int			max_entries;
} PwetLWLockHash;

/*
 * Per-backend statistics payload, allocated in the stats DSA area only for
 * a backend that has enabled capture.  Fixed-class entries are followed by
 * the runtime-sized LWLock hash and its entry array.
 */
typedef struct PwetStats
{
	pg_atomic_uint32 reset_generation;
	instr_time	wait_start;
	uint32		current_event;
	int64		reset_count;
	PwetTimingEntry events[PWET_NUM_EVENTS];
	PwetLWLockHash lwlock_hash;
	int64		lwlock_overflow_count;
	int64		flat_overflow_count;
} PwetStats;

/*
 * One entry per possible ProcNumber, always resident in the control
 * segment.  trace_ptr/trace_state are unused placeholders reserved for the
 * trace-level patch.
 */
typedef struct PwetSlot
{
	dsa_pointer stats_ptr;		/* InvalidDsaPointer when not collecting */
	dsa_pointer trace_ptr;		/* reserved for the trace level */
	uint8		trace_state;	/* reserved for the trace level */
	pg_atomic_uint32 generation;	/* bumped on every ownership change */
} PwetSlot;

typedef struct PwetControl
{
	LWLock		lock;
	PwetSlot	slots[FLEXIBLE_ARRAY_MEMBER];
} PwetControl;

static const struct config_enum_entry pwet_capture_options[] = {
	{"off", PWET_CAPTURE_OFF, false},
	{"stats", PWET_CAPTURE_STATS, false},
	{NULL, 0, false}
};

static int	pwet_capture = PWET_CAPTURE_OFF;
static int	pwet_max_tranches = 192;

static PwetControl *pwet_ctl;
static dsa_area *pwet_stats_dsa;

static PwetStats *pwet_my_stats;
static ProcNumber pwet_my_procno = INVALID_PROC_NUMBER;
static Size pwet_stats_stride;
static uint32 pwet_last_reset_generation;

static bool pwet_active;
static bool pwet_attach_needed;
static bool pwet_exit_started;
static bool pwet_stats_writes_disabled;
static bool pwet_exit_callback_registered;

static wait_event_hook_type prev_wait_event_begin_hook;
static wait_event_hook_type prev_wait_event_end_hook;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook;
static ExecutorStart_hook_type prev_ExecutorStart_hook;

static void pwet_wait_begin(uint32 wait_event_info);
static void pwet_wait_end(uint32 wait_event_info);
static void pwet_maybe_attach(void);
static bool pwet_ensure_control(void);
static bool pwet_ensure_stats_dsa(void);
static void pwet_release_stats(void);
static void pwet_before_shmem_exit(int code, Datum arg);
static void pwet_request_reset(int procnumber);

static Size
pwet_control_size(int nslots)
{
	return add_size(offsetof(PwetControl, slots),
					mul_size(nslots, sizeof(PwetSlot)));
}

static int
pwet_hash_size_for(int max_entries)
{
	int			size = 32;

	while (size < max_entries * 2)
		size <<= 1;
	return size;
}

static Size
pwet_stats_payload_size(int max_entries)
{
	int			hash_size = pwet_hash_size_for(max_entries);

	return add_size(sizeof(PwetStats),
					add_size(mul_size(hash_size,
									  sizeof(PwetLWLockHashEntry)),
							 mul_size(max_entries,
									  sizeof(PwetTimingEntry))));
}

static inline PwetLWLockHashEntry *
pwet_lwlock_hash_entries(PwetStats *state)
{
	return (PwetLWLockHashEntry *)
		((char *) state + sizeof(PwetStats));
}

static inline PwetTimingEntry *
pwet_lwlock_hash_events(PwetStats *state)
{
	return (PwetTimingEntry *)
		((char *) state + sizeof(PwetStats) +
		 (Size) state->lwlock_hash.hash_size *
		 sizeof(PwetLWLockHashEntry));
}

static void
pwet_lwlock_hash_clear(PwetStats *state)
{
	PwetLWLockHash *hash = &state->lwlock_hash;
	PwetLWLockHashEntry *entries = pwet_lwlock_hash_entries(state);
	PwetTimingEntry *events = pwet_lwlock_hash_events(state);
	int			i;

	hash->num_used = 0;
	memset(events, 0,
		   (Size) hash->max_entries * sizeof(PwetTimingEntry));
	for (i = 0; i < hash->hash_size; i++)
	{
		entries[i].tranche_id = PWET_LWLOCK_EMPTY;
		entries[i].dense_idx = 0;
	}
}

static PwetTimingEntry *
pwet_lwlock_lookup(PwetStats *state, uint16 tranche_id)
{
	PwetLWLockHash *hash = &state->lwlock_hash;
	PwetLWLockHashEntry *entries = pwet_lwlock_hash_entries(state);
	PwetTimingEntry *events = pwet_lwlock_hash_events(state);
	uint32		hash_value = (uint32) tranche_id * 2654435761U;
	int			slot = hash_value & (hash->hash_size - 1);
	int			limit;
	int			i;

	limit = hash->num_used >= hash->max_entries
		? PWET_LWLOCK_PROBE_LIMIT : hash->hash_size;

	for (i = 0; i < limit; i++)
	{
		PwetLWLockHashEntry *entry = &entries[slot];

		if (entry->tranche_id == tranche_id)
			return &events[entry->dense_idx];

		if (entry->tranche_id == PWET_LWLOCK_EMPTY)
		{
			if (hash->num_used >= hash->max_entries)
				return NULL;

			entry->tranche_id = tranche_id;
			entry->dense_idx = hash->num_used++;
			return &events[entry->dense_idx];
		}

		slot = (slot + 1) & (hash->hash_size - 1);
	}

	return NULL;
}

static int
pwet_timing_index(uint32 wait_event_info)
{
	uint32		class_id = wait_event_info & PWET_WAIT_EVENT_CLASS_MASK;
	int			event_id = wait_event_info & PWET_WAIT_EVENT_ID_MASK;
	int			class_byte;
	int			dense;

	if (class_id == PG_WAIT_LWLOCK)
		return PWET_IDX_LWLOCK;

	class_byte = class_id >> 24;
	if (class_byte >= PWET_RAW_CLASSES)
		return -1;

	dense = pwet_class_dense[class_byte];
	if (dense < 0 || event_id >= pwet_class_nevents[dense])
		return -1;

	return pwet_class_offset[dense] + event_id;
}

static int
pwet_timing_bucket(int64 duration_ns)
{
	int			bucket;

	if (duration_ns < 1024)
		return 0;

	bucket = pg_leftmost_one_pos64((uint64) duration_ns) - 9;
	if (bucket >= PWET_HISTOGRAM_BUCKETS)
		bucket = PWET_HISTOGRAM_BUCKETS - 1;
	return bucket;
}

/*
 * GetNamedDSMSegment() init callback for the control segment: allocate an
 * LWLock tranche id for the embedded lock and mark every slot empty.
 */
static void
pwet_control_init(void *ptr, void *arg)
{
	PwetControl *ctl = (PwetControl *) ptr;
	int			tranche_id = LWLockNewTrancheId(PWET_CONTROL_NAME);
	int			i;

	LWLockInitialize(&ctl->lock, tranche_id);
	for (i = 0; i < PWET_NUM_SLOTS; i++)
	{
		ctl->slots[i].stats_ptr = InvalidDsaPointer;
		ctl->slots[i].trace_ptr = InvalidDsaPointer;
		ctl->slots[i].trace_state = PWET_TRACE_FREE;
		pg_atomic_init_u32(&ctl->slots[i].generation, 0);
	}
}

/*
 * Lazily create or attach the control segment.  Safe to call from any
 * backend at any safe point; never called from the begin/end hooks.
 */
static bool
pwet_ensure_control(void)
{
	bool		found;

	if (pwet_ctl != NULL)
		return true;

	pwet_ctl = (PwetControl *) GetNamedDSMSegment(PWET_CONTROL_NAME,
												  pwet_control_size(PWET_NUM_SLOTS),
												  pwet_control_init,
												  &found,
												  NULL);
	return pwet_ctl != NULL;
}

/*
 * Lazily attach this backend to the stats DSA area.  GetNamedDSA() manages
 * its own tranche and creation lock; we only need to remember the result.
 */
static bool
pwet_ensure_stats_dsa(void)
{
	bool		found;

	if (pwet_stats_dsa != NULL)
		return true;

	pwet_stats_dsa = GetNamedDSA(PWET_STATS_DSA_NAME, &found);
	return pwet_stats_dsa != NULL;
}

static bool
pwet_can_attach(void)
{
	if (pwet_exit_started || !pwet_active || pwet_capture == PWET_CAPTURE_OFF)
		return false;
	if (MyProc == NULL || MyProcNumber == INVALID_PROC_NUMBER)
		return false;
	if (MyProcNumber < 0 || MyProcNumber >= PWET_NUM_SLOTS)
		return false;
	if (!IsNormalProcessingMode() || CritSectionCount > 0)
		return false;
	if (MyProc->lwWaiting != LW_WS_NOT_WAITING)
		return false;
	return true;
}

static bool
pwet_attach_stats(void)
{
	static bool in_attach;
	PwetSlot   *slot;
	PwetStats  *state = NULL;
	dsa_pointer stats_ptr = InvalidDsaPointer;

	if (pwet_my_stats != NULL)
		return true;
	if (in_attach || !pwet_can_attach())
		return false;

	in_attach = true;
	PG_TRY();
	{
		if (pwet_ensure_control() && pwet_ensure_stats_dsa())
		{
			PwetLWLockHashEntry *entries;
			int			hash_size;
			int			i;

			pwet_stats_stride = pwet_stats_payload_size(pwet_max_tranches);
			hash_size = pwet_hash_size_for(pwet_max_tranches);
			stats_ptr = dsa_allocate_extended(pwet_stats_dsa,
											  pwet_stats_stride,
											  DSA_ALLOC_ZERO |
											  DSA_ALLOC_NO_OOM);
			if (DsaPointerIsValid(stats_ptr))
			{
				state = dsa_get_address(pwet_stats_dsa, stats_ptr);
				pg_atomic_init_u32(&state->reset_generation, 0);
				state->lwlock_hash.num_used = 0;
				state->lwlock_hash.hash_size = hash_size;
				state->lwlock_hash.max_entries = pwet_max_tranches;
				entries = pwet_lwlock_hash_entries(state);
				for (i = 0; i < hash_size; i++)
					entries[i].tranche_id = PWET_LWLOCK_EMPTY;

				slot = &pwet_ctl->slots[MyProcNumber];
				LWLockAcquire(&pwet_ctl->lock, LW_EXCLUSIVE);
				if (DsaPointerIsValid(slot->stats_ptr))
					dsa_free(pwet_stats_dsa, slot->stats_ptr);
				slot->stats_ptr = stats_ptr;
				pg_atomic_fetch_add_u32(&slot->generation, 1);
				LWLockRelease(&pwet_ctl->lock);

				pwet_my_stats = state;
				pwet_my_procno = MyProcNumber;
				pwet_last_reset_generation = 0;
			}
		}
	}
	PG_FINALLY();
	{
		in_attach = false;
	}
	PG_END_TRY();

	return pwet_my_stats != NULL;
}

static void
pwet_maybe_attach(void)
{
	if (!pwet_attach_needed || !pwet_can_attach())
		return;

	if (!pwet_attach_stats())
		return;

	/*
	 * Registered lazily, per backend, the first time that backend actually
	 * attaches: on_exit_reset() (called early in every forked/exec'd
	 * backend, well before shared_preload_libraries processing happens
	 * again on EXEC_BACKEND, and inherited as a no-op on fork otherwise)
	 * would discard a registration made from _PG_init() in the postmaster,
	 * so this is the only place this can usefully happen.
	 */
	if (!pwet_exit_callback_registered)
	{
		before_shmem_exit(pwet_before_shmem_exit, (Datum) 0);
		pwet_exit_callback_registered = true;
	}

	pwet_attach_needed = false;
}

static void
pwet_release_stats(void)
{
	PwetSlot   *slot;
	ProcNumber	procno = pwet_my_procno;
	bool		was_disabled = pwet_stats_writes_disabled;

	if (pwet_my_stats == NULL || pwet_stats_dsa == NULL || pwet_ctl == NULL ||
		procno == INVALID_PROC_NUMBER)
	{
		pwet_my_stats = NULL;
		return;
	}

	pwet_stats_writes_disabled = true;
	pwet_my_stats = NULL;
	slot = &pwet_ctl->slots[procno];

	LWLockAcquire(&pwet_ctl->lock, LW_EXCLUSIVE);
	if (DsaPointerIsValid(slot->stats_ptr))
	{
		dsa_free(pwet_stats_dsa, slot->stats_ptr);
		slot->stats_ptr = InvalidDsaPointer;
		pg_atomic_fetch_add_u32(&slot->generation, 1);
	}
	LWLockRelease(&pwet_ctl->lock);

	if (!pwet_exit_started)
		pwet_stats_writes_disabled = was_disabled;
}

static void
pwet_before_shmem_exit(int code, Datum arg)
{
	pwet_exit_started = true;
	pwet_stats_writes_disabled = true;
	pwet_release_stats();
	pwet_my_procno = INVALID_PROC_NUMBER;
}

static void
pwet_assign_capture(int newval, void *extra)
{
	if (pwet_my_stats != NULL)
	{
		INSTR_TIME_SET_ZERO(pwet_my_stats->wait_start);
		pwet_my_stats->current_event = 0;
	}

	if (!pwet_active || pwet_exit_started)
		return;

	if (newval == PWET_CAPTURE_OFF)
	{
		pwet_release_stats();
		pwet_attach_needed = false;
	}
	else
	{
		pwet_attach_needed = true;
		/* Attach right away if this is a safe point; otherwise the
		 * post_parse_analyze/ExecutorStart hooks pick it up. */
		if (IsNormalProcessingMode())
			pwet_maybe_attach();
	}
}

static void
pwet_wait_begin(uint32 wait_event_info)
{
	if (prev_wait_event_begin_hook != NULL)
		prev_wait_event_begin_hook(wait_event_info);

	if (pwet_capture == PWET_CAPTURE_OFF ||
		pwet_stats_writes_disabled || pwet_my_stats == NULL)
		return;

	INSTR_TIME_SET_CURRENT(pwet_my_stats->wait_start);
	pwet_my_stats->current_event = wait_event_info;
}

static void
pwet_wait_end(uint32 wait_event_info)
{
	PwetStats  *state = pwet_my_stats;

	if (pwet_capture != PWET_CAPTURE_OFF &&
		!pwet_stats_writes_disabled && state != NULL)
	{
		uint32		event = state->current_event;
		uint32		reset_generation;

		reset_generation = pg_atomic_read_u32(&state->reset_generation);
		if (reset_generation != pwet_last_reset_generation)
		{
			memset(state->events, 0, sizeof(state->events));
			pwet_lwlock_hash_clear(state);
			state->reset_count++;
			state->lwlock_overflow_count = 0;
			state->flat_overflow_count = 0;
			state->current_event = 0;
			pwet_last_reset_generation = reset_generation;
		}

		if (event != 0 && !INSTR_TIME_IS_ZERO(state->wait_start))
		{
			instr_time	now;
			int64		duration_ns;
			int			idx;
			PwetTimingEntry *entry = NULL;

			INSTR_TIME_SET_CURRENT(now);
			duration_ns = INSTR_TIME_GET_NANOSEC(now) -
				INSTR_TIME_GET_NANOSEC(state->wait_start);
			if (duration_ns < 0)
				duration_ns = 0;

			idx = pwet_timing_index(event);
			if (idx == PWET_IDX_LWLOCK)
				entry = pwet_lwlock_lookup(state,
										   event &
										   PWET_WAIT_EVENT_ID_MASK);
			else if (idx >= 0)
				entry = &state->events[idx];

			if (entry != NULL)
			{
				entry->count++;
				entry->total_ns += duration_ns;
				if (duration_ns > entry->max_ns)
					entry->max_ns = duration_ns;
				entry->histogram[pwet_timing_bucket(duration_ns)]++;
			}
			else if (idx == PWET_IDX_LWLOCK)
				state->lwlock_overflow_count++;
			else
				state->flat_overflow_count++;

			INSTR_TIME_SET_ZERO(state->wait_start);
		}
	}

	if (prev_wait_event_end_hook != NULL)
		prev_wait_event_end_hook(wait_event_info);
}

static void
pwet_post_parse_analyze(ParseState *pstate, Query *query,
						const JumbleState *jstate)
{
	if (prev_post_parse_analyze_hook != NULL)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	pwet_maybe_attach();
}

static void
pwet_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	pwet_maybe_attach();

	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * Resolve the optional pid SRF argument to a ProcNumber range
 * [out_start, out_end).  Returns false if the SRF should emit zero rows
 * (unknown pid -- silent no-op).  Auxiliary processes are included here:
 * unlike the reset functions, reading their stats is not a control action.
 */
static bool
pwet_pid_range(FunctionCallInfo fcinfo, int argnum,
			  int *out_start, int *out_end)
{
	if (PG_ARGISNULL(argnum))
	{
		*out_start = 0;
		*out_end = PWET_NUM_SLOTS;
		return true;
	}
	else
	{
		int			target_pid = PG_GETARG_INT32(argnum);
		PGPROC	   *proc;
		int			procnumber;

		proc = BackendPidGetProc(target_pid);
		if (proc == NULL)
			proc = AuxiliaryPidGetProc(target_pid);
		if (proc == NULL)
			return false;

		procnumber = GetNumberFromPGProc(proc);
		if (procnumber < 0 || procnumber >= PWET_NUM_SLOTS)
			return false;

		*out_start = procnumber;
		*out_end = procnumber + 1;
		return true;
	}
}

static void
pwet_emit_timing_row(ReturnSetInfo *rsinfo, PgBackendStatus *beentry,
					 int procnumber, uint32 wait_event_info,
					 PwetTimingEntry *entry, ArrayType *histogram,
					 int64 *histogram_data)
{
	Datum		values[10];
	bool		nulls[10] = {0};
	const char *event_type;
	const char *event_name;
	int			i;

	event_type = pgstat_get_wait_event_type(wait_event_info);
	event_name = pgstat_get_wait_event(wait_event_info);
	if (event_type == NULL || event_name == NULL)
		return;

	values[0] = Int32GetDatum(beentry->st_procpid);
	values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
	values[2] = Int32GetDatum(procnumber);
	values[3] = CStringGetTextDatum(event_type);
	values[4] = CStringGetTextDatum(event_name);
	values[5] = Int64GetDatum(entry->count);
	values[6] = Float8GetDatum((double) entry->total_ns / 1000000.0);
	values[7] = Float8GetDatum(entry->count > 0
							   ? (double) entry->total_ns /
							   entry->count / 1000.0 : 0.0);
	values[8] = Float8GetDatum((double) entry->max_ns / 1000.0);
	for (i = 0; i < PWET_HISTOGRAM_BUCKETS; i++)
		histogram_data[i] = entry->histogram[i];
	values[9] = PointerGetDatum(histogram);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * SQL function: pg_stat_get_wait_event_timing(pid int4, OUT ...)
 *
 * One row per (backend, wait_event) with a non-zero count.  pid is
 * optional: NULL means every backend; a non-NULL value restricts the sweep
 * to that backend (silently empty for an unknown pid).
 */
Datum
pg_stat_get_wait_event_timing(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	ArrayType  *histogram;
	int64	   *histogram_data;
	PwetStats  *snapshot;
	int			start_idx;
	int			end_idx;
	int			procnumber;

	InitMaterializedSRF(fcinfo, 0);

	if (!pwet_pid_range(fcinfo, 0, &start_idx, &end_idx))
		PG_RETURN_VOID();
	if (!pwet_ensure_control() || !pwet_ensure_stats_dsa())
		PG_RETURN_VOID();

	pwet_stats_stride = pwet_stats_payload_size(pwet_max_tranches);
	snapshot = palloc(pwet_stats_stride);

	{
		Datum		zeros[PWET_HISTOGRAM_BUCKETS];

		memset(zeros, 0, sizeof(zeros));
		histogram = construct_array_builtin(zeros,
											PWET_HISTOGRAM_BUCKETS,
											INT8OID);
		histogram_data = (int64 *) ARR_DATA_PTR(histogram);
	}

	for (procnumber = start_idx; procnumber < end_idx; procnumber++)
	{
		PwetSlot   *slot = &pwet_ctl->slots[procnumber];
		PgBackendStatus *beentry;
		dsa_pointer stats_ptr;
		int			i;

		beentry = pgstat_get_beentry_by_proc_number(procnumber);
		if (beentry == NULL || beentry->st_procpid == 0 ||
			!PWET_HAS_STATS_PRIVS(beentry->st_userid))
			continue;

		LWLockAcquire(&pwet_ctl->lock, LW_SHARED);
		stats_ptr = slot->stats_ptr;
		if (!DsaPointerIsValid(stats_ptr))
		{
			LWLockRelease(&pwet_ctl->lock);
			continue;
		}
		memcpy(snapshot, dsa_get_address(pwet_stats_dsa, stats_ptr),
			   pwet_stats_stride);
		LWLockRelease(&pwet_ctl->lock);

		for (i = 0; i < PWET_DENSE_CLASSES; i++)
		{
			int			base = pwet_class_offset[i];
			int			nevents = pwet_class_nevents[i];
			uint32		class_id = pwet_dense_to_classid[i];
			int			j;

			for (j = 0; j < nevents; j++)
			{
				PwetTimingEntry *entry = &snapshot->events[base + j];

				if (entry->count == 0)
					continue;
				pwet_emit_timing_row(rsinfo, beentry, procnumber,
									 ((uint32) class_id << 24) | (uint32) j,
									 entry, histogram, histogram_data);
			}
		}

		{
			PwetLWLockHashEntry *entries =
				pwet_lwlock_hash_entries(snapshot);
			PwetTimingEntry *events =
				pwet_lwlock_hash_events(snapshot);

			for (i = 0; i < snapshot->lwlock_hash.hash_size; i++)
			{
				PwetLWLockHashEntry *hash_entry = &entries[i];
				PwetTimingEntry *entry;

				if (hash_entry->tranche_id == PWET_LWLOCK_EMPTY)
					continue;
				entry = &events[hash_entry->dense_idx];
				if (entry->count == 0)
					continue;
				pwet_emit_timing_row(rsinfo, beentry, procnumber,
									 PG_WAIT_LWLOCK |
									 hash_entry->tranche_id,
									 entry, histogram, histogram_data);
			}
		}
	}

	pfree(snapshot);
	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_get_wait_event_timing_overflow(pid int4, OUT ...)
 *
 * One row per backend that has an attached stats payload, exposing the
 * truncation counters the recording path maintains.  pid has the same
 * optional semantics as pg_stat_get_wait_event_timing().
 */
Datum
pg_stat_get_wait_event_timing_overflow(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			start_idx;
	int			end_idx;
	int			procnumber;

	InitMaterializedSRF(fcinfo, 0);

	if (!pwet_pid_range(fcinfo, 0, &start_idx, &end_idx))
		PG_RETURN_VOID();
	if (!pwet_ensure_control() || !pwet_ensure_stats_dsa())
		PG_RETURN_VOID();

	for (procnumber = start_idx; procnumber < end_idx; procnumber++)
	{
		PwetSlot   *slot = &pwet_ctl->slots[procnumber];
		PgBackendStatus *beentry;
		Datum		values[6];
		bool		nulls[6] = {0};
		int64		lwlock_overflow = 0;
		int64		flat_overflow = 0;
		int64		reset_count = 0;
		bool		matched = false;

		beentry = pgstat_get_beentry_by_proc_number(procnumber);
		if (beentry == NULL || beentry->st_procpid == 0 ||
			!PWET_HAS_STATS_PRIVS(beentry->st_userid))
			continue;

		LWLockAcquire(&pwet_ctl->lock, LW_SHARED);
		if (DsaPointerIsValid(slot->stats_ptr))
		{
			PwetStats  *state = dsa_get_address(pwet_stats_dsa,
												slot->stats_ptr);

			lwlock_overflow = state->lwlock_overflow_count;
			flat_overflow = state->flat_overflow_count;
			reset_count = state->reset_count;
			matched = true;
		}
		LWLockRelease(&pwet_ctl->lock);

		if (!matched)
			continue;

		values[0] = Int32GetDatum(beentry->st_procpid);
		values[1] = CStringGetTextDatum(GetBackendTypeDesc(beentry->st_backendType));
		values[2] = Int32GetDatum(procnumber);
		values[3] = Int64GetDatum(lwlock_overflow);
		values[4] = Int64GetDatum(flat_overflow);
		values[5] = Int64GetDatum(reset_count);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	PG_RETURN_VOID();
}

static void
pwet_reset_own(void)
{
	if (pwet_my_stats != NULL)
	{
		memset(pwet_my_stats->events, 0, sizeof(pwet_my_stats->events));
		pwet_lwlock_hash_clear(pwet_my_stats);
		pwet_my_stats->reset_count++;
		pwet_my_stats->lwlock_overflow_count = 0;
		pwet_my_stats->flat_overflow_count = 0;
		pwet_my_stats->current_event = 0;
		INSTR_TIME_SET_ZERO(pwet_my_stats->wait_start);
	}
}

/*
 * Request an asynchronous reset on the given slot's stats payload, if it
 * has one.  The owning backend notices at its next wait_end (see
 * pwet_wait_end()) and clears its own counters.  Holding the control
 * lock in shared mode for the whole read-then-bump excludes a concurrent
 * pwet_release_stats()/dsa_free() on the same slot.
 */
static void
pwet_request_reset(int procnumber)
{
	PwetSlot   *slot;
	dsa_pointer stats_ptr;

	if (!pwet_ensure_control() || !pwet_ensure_stats_dsa())
		return;

	slot = &pwet_ctl->slots[procnumber];

	LWLockAcquire(&pwet_ctl->lock, LW_SHARED);
	stats_ptr = slot->stats_ptr;
	if (DsaPointerIsValid(stats_ptr))
	{
		PwetStats  *state = dsa_get_address(pwet_stats_dsa, stats_ptr);

		pg_atomic_fetch_add_u32(&state->reset_generation, 1);
	}
	LWLockRelease(&pwet_ctl->lock);
}

/*
 * SQL function: pg_stat_reset_wait_event_timing(pid int4)
 *
 *   NULL or own pid : reset the caller's own counters synchronously.
 *   another pid     : request a cross-backend reset (pg_signal_backend).
 *   unknown pid     : silent no-op.
 */
Datum
pg_stat_reset_wait_event_timing(PG_FUNCTION_ARGS)
{
	int			target_pid;
	PGPROC	   *proc;
	int			procnumber;

	if (PG_ARGISNULL(0) || PG_GETARG_INT32(0) == MyProcPid)
	{
		pwet_reset_own();
		PG_RETURN_VOID();
	}

	/*
	 * Cross-backend reset requires pg_signal_backend, matching
	 * pg_stat_reset_backend_stats(pid).
	 */
	if (!has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to reset another backend's wait event timing statistics"),
				 errdetail("Only roles with privileges of the \"pg_signal_backend\" role may reset another backend's wait event timing statistics.")));

	target_pid = PG_GETARG_INT32(0);

	proc = BackendPidGetProc(target_pid);
	if (proc == NULL)
		proc = AuxiliaryPidGetProc(target_pid);
	if (proc == NULL)
		PG_RETURN_VOID();		/* unknown/dead pid: silent no-op */

	procnumber = GetNumberFromPGProc(proc);
	if (procnumber < 0 || procnumber >= PWET_NUM_SLOTS)
		PG_RETURN_VOID();

	pwet_request_reset(procnumber);

	PG_RETURN_VOID();
}

/*
 * SQL function: pg_stat_reset_wait_event_timing_all()
 *
 * Request a reset on every slot.  Execution is revoked from PUBLIC in the
 * extension script; administrators can delegate with GRANT.
 */
Datum
pg_stat_reset_wait_event_timing_all(PG_FUNCTION_ARGS)
{
	int			i;

	for (i = 0; i < PWET_NUM_SLOTS; i++)
		pwet_request_reset(i);

	PG_RETURN_VOID();
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_wait_event_tracing must be loaded via \"shared_preload_libraries\"")));

	DefineCustomEnumVariable("pg_wait_event_tracing.capture",
							 "Controls wait event collection.",
							 NULL,
							 &pwet_capture,
							 PWET_CAPTURE_OFF,
							 pwet_capture_options,
							 PGC_SUSET,
							 GUC_NOT_IN_SAMPLE,
							 NULL,
							 pwet_assign_capture,
							 NULL);
	DefineCustomIntVariable("pg_wait_event_tracing.max_tranches",
							"Maximum distinct LWLock tranches tracked per backend.",
							NULL,
							&pwet_max_tranches,
							192,
							16,
							65534,
							PGC_POSTMASTER,
							GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);
	MarkGUCPrefixReserved("pg_wait_event_tracing");

	prev_wait_event_begin_hook = wait_event_begin_hook;
	prev_wait_event_end_hook = wait_event_end_hook;
	wait_event_begin_hook = pwet_wait_begin;
	wait_event_end_hook = pwet_wait_end;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pwet_post_parse_analyze;
	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = pwet_ExecutorStart;

	pwet_active = true;
	pwet_attach_needed = (pwet_capture != PWET_CAPTURE_OFF);
}
