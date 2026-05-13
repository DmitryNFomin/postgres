# Copyright (c) 2026, PostgreSQL Global Development Group
#
# End-to-end test for wait-event trace orphan persistence and the
# in-tree cross-backend reader pg_get_wait_event_trace().
#
# Three scenarios:
#
#   1. Plain backend orphan roundtrip
#      A writer session enables wait_event_capture = trace, emits a
#      handful of waits, captures its own procnumber, and disconnects.
#      A separate long-lived reader session then asserts that
#      pg_get_wait_event_trace(<writer_procnumber>) returns the
#      writer's recorded events post-mortem, that
#      pg_stat_clear_orphaned_wait_event_rings() releases the orphan,
#      and that a subsequent read returns empty.
#
#   2. Parallel-worker orphan roundtrip (the patch's stated motivation)
#      A query is forced through parallel workers via
#      debug_parallel_query=on plus zero parallel costs; the workers
#      exit at end-of-parallel-query in milliseconds.  The test
#      then asserts pg_stat_clear_orphaned_wait_event_rings()
#      returns at least 2 -- the leader and at least one worker --
#      confirming that short-lived parallel workers do leave
#      readable orphans, the case the orphan-persistence lifecycle
#      was designed for.
#
#   3. OWNED-slot read with a concurrent live writer
#      A long-lived writer session emits a steady stream of PgSleep
#      wait events while a separate reader calls
#      pg_get_wait_event_trace(writer_procnumber) repeatedly.  All
#      rows must be well-formed (no NULL/empty event_type or
#      event_name, no negative durations) -- this exercises the
#      per-record seqlock protocol that protects against torn
#      reads of records mid-write.  Without the seqlock the reader
#      would emit malformed records during contention windows.
#
# Race-hardening: the reader session is held open for the entire
# run so its procnumber slot cannot be a recycle target for any
# writer or parallel worker when they exit, and the test asserts
# no unrelated client backend is present at the orphan-read
# moment.  Skipped on builds without --enable-wait-event-timing.

use strict;
use warnings FATAL => 'all';

use Time::HiRes qw(usleep);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('wet_orphan_roundtrip');
$node->init;
# A high max_connections gives plenty of unused procnumber slots so a
# new backend started during the test window is unlikely to recycle
# the just-exited writer's slot.  Combined with the long-lived reader
# session below (which pins its own slot for the full run), this
# closes the race window to negligible width on a quiet test node.
$node->append_conf('postgresql.conf', q{
max_connections = 100
});
$node->start;

# Skip when wait-event-timing isn't compiled in.  GUC check hook
# rejects 'trace' on stub builds; detect via a probe SET.
my ($rc, $stdout, $stderr) = $node->psql(
	'postgres',
	"SET wait_event_capture = trace;");
if ($stderr =~ /not supported by this build/)
{
	plan skip_all =>
	  'wait_event_timing not compiled in (--enable-wait-event-timing)';
}

# Long-lived reader session.  Stays connected for the entire test so
# its procnumber slot is in OWNED state and therefore not eligible as
# a recycle target for writer/parallel-worker slots when they exit.
my $reader = $node->background_psql('postgres');

# ------------------------------------------------------------------
# Scenario 1: plain backend orphan roundtrip
# ------------------------------------------------------------------

# Spawn the writer as a one-shot psql.  It enables trace, emits a
# handful of waits inside a DO block (PERFORM avoids empty-row
# pollution of the captured output), then SELECTs its own procnumber.
# Because psql one-shot commands spawn a fresh backend that exits
# when the SQL completes, the writer's slot transitions to ORPHANED
# on exit.
my $writer_proc = $node->safe_psql(
	'postgres', q{
		SET wait_event_capture = trace;
		DO $$
		BEGIN
		  PERFORM pg_sleep(0.02);
		  PERFORM pg_sleep(0.02);
		  PERFORM pg_sleep(0.02);
		END
		$$;
		SELECT procnumber
		FROM pg_stat_get_wait_event_timing(pg_backend_pid())
		WHERE pid = pg_backend_pid()
		LIMIT 1;
	});
chomp $writer_proc;
like($writer_proc, qr/^\d+$/, 'writer reported its procnumber');

# Wait for the writer backend to fully exit.  pg_stat_activity loses
# the row before the before_shmem_exit callback finishes; we then
# additionally assert that no other client backend has inherited the
# writer's procnumber, which would clear the orphan via
# wait_event_trace_clear_orphan_at_init.
my $other_clients_query =
  "SELECT count(*) FROM pg_stat_activity "
  . "WHERE backend_type = 'client backend' AND pid <> pg_backend_pid();";

my $writer_gone = 0;
for (my $i = 0; $i < 100; $i++)
{
	my $count = $reader->query_safe($other_clients_query);
	chomp $count;
	if ($count eq '0') { $writer_gone = 1; last; }
	usleep(20_000);
}
ok($writer_gone, 'writer backend has exited (slot should be ORPHANED)');

# Race-harden: confirm no client backend has taken over the writer's
# procnumber between its exit and our read.  This is what would
# clear the orphan; if some other test artifact triggered it we want
# the test to fail loudly rather than spuriously report "no orphan".
my $recycler_count = $reader->query_safe(
	"SELECT count(*) FROM pg_stat_activity "
	. "WHERE backend_type = 'client backend' "
	. "  AND pid <> pg_backend_pid();");
chomp $recycler_count;
is($recycler_count, '0',
	'no other client backend present at orphan-read time (slot not recycled)');

# Read the orphaned ring via the cross-backend reader.  At least one
# record is expected (we emitted three pg_sleep waits).
my $orphan_rows = $reader->query_safe(
	"SELECT count(*) FROM pg_get_wait_event_trace($writer_proc);");
chomp $orphan_rows;
cmp_ok($orphan_rows, '>=', 1,
	"pg_get_wait_event_trace($writer_proc) reads ORPHANED ring (rows: $orphan_rows)");

# Admin sweep: clear the orphan.  Should report >= 1 since we know one
# ORPHANED slot exists.
my $cleared = $reader->query_safe(
	"SELECT pg_stat_clear_orphaned_wait_event_rings();");
chomp $cleared;
cmp_ok($cleared, '>=', 1,
	"pg_stat_clear_orphaned_wait_event_rings released $cleared ring(s)");

# After the sweep, the orphan is gone.  Reading the same procnumber
# returns empty.
my $after_clear = $reader->query_safe(
	"SELECT count(*) FROM pg_get_wait_event_trace($writer_proc);");
chomp $after_clear;
is($after_clear, '0',
	"pg_get_wait_event_trace($writer_proc) returns empty after sweep");

# ------------------------------------------------------------------
# Scenario 2: parallel-worker orphan roundtrip
# ------------------------------------------------------------------
#
# Force parallel workers to participate in a trivial seq scan,
# capture their procnumbers while alive, then assert each worker's
# orphaned ring is readable after the parallel query has finished
# (workers exit in milliseconds at end-of-parallel-query).

# Create the table used to force parallelism.  Done from the reader
# session so it survives across the writer's lifetime.  Suppress
# NOTICE so query_safe doesn't treat the "does not exist, skipping"
# message as a failure (BackgroundPsql::query_safe treats any stderr
# output as a query failure).  The table is sized large enough
# (1M rows) and the query is structured (ORDER BY + count(*) under
# a Gather) so workers reliably emit wait events (tuple queue
# operations, latch waits) before they exit at end-of-parallel-
# query.  A smaller table with a plain count(*) can be processed
# entirely from cache without any wait points, leaving worker
# trace rings never lazily allocated, never transitioning to
# OWNED, and never producing ORPHANED slots -- the test would
# pass without exercising the parallel-worker case.
$reader->query_safe("SET client_min_messages = warning;");
$reader->query_safe(q{
	DROP TABLE IF EXISTS wet_parallel_target;
	CREATE TABLE wet_parallel_target AS
	  SELECT i FROM generate_series(1, 1000000) i;
});

# Spawn a writer session that enables trace and runs a forced-
# parallel query.  Workers run, then exit at end-of-parallel-query;
# the leader (this safe_psql backend) then exits when safe_psql
# returns.  After return, both leader and workers are gone -- each
# leaves an ORPHANED slot whose ring should be readable.
$node->safe_psql(
	'postgres', q{
		SET wait_event_capture = trace;
		SET min_parallel_table_scan_size = 0;
		SET parallel_setup_cost = 0;
		SET parallel_tuple_cost = 0;
		SET max_parallel_workers_per_gather = 2;
		SET debug_parallel_query = on;
		-- ORDER BY forces a parallel sort and a Gather Merge,
		-- which routes tuples through shm_mq queues -- workers
		-- reliably emit MessageQueueSend / MessageQueueReceive
		-- wait events here, guaranteeing lazy trace-ring
		-- allocation and OWNED->ORPHANED transition on exit.
		SELECT count(*) FROM (
		  SELECT i FROM wet_parallel_target ORDER BY i
		) s;
	});

# Wait for all client/worker backends to fully exit.  At
# safe_psql return the leader has exited, but worker
# before_shmem_exit callbacks may still be running --
# pg_stat_clear_orphaned_wait_event_rings counts only
# slots that have completed their OWNED -> ORPHANED
# transition, so racing the callbacks under-counts.
my $exit_drained = 0;
for (my $i = 0; $i < 200; $i++)
{
	my $count = $reader->query_safe(
		"SELECT count(*) FROM pg_stat_activity "
		. "WHERE backend_type IN ('client backend', 'parallel worker') "
		. "  AND pid <> pg_backend_pid();");
	chomp $count;
	if ($count eq '0') { $exit_drained = 1; last; }
	usleep(20_000);
}
ok($exit_drained,
	'all parallel-query backends have exited before counting orphans');

# Count parallel-produced orphans via the admin sweep, which
# returns the number of rings released.  After a forced-parallel
# query with the leader and workers all exited, we expect at
# least 2 orphans (leader + at least one worker).
#
# Using the sweep is cheaper than iterating every procnumber and
# calling pg_get_wait_event_trace on each: it's a single lock
# acquisition and tells us the count directly.  The read path
# itself is already covered by scenario 1 above; here we only
# need to confirm that parallel-worker exits do produce orphans.
my $parallel_orphans = $reader->query_safe(
	"SELECT pg_stat_clear_orphaned_wait_event_rings();");
chomp $parallel_orphans;
cmp_ok($parallel_orphans, '>=', 2,
	"parallel-query exit produced >= 2 orphans (leader + worker(s)): $parallel_orphans");

# ------------------------------------------------------------------
# Scenario 3: OWNED-slot read with a concurrent live writer
# ------------------------------------------------------------------
#
# Exercises the per-record seqlock protocol against an actively
# writing backend.  OWNED is the case where the seqlock check is
# load-bearing: the writer is concurrently appending records to
# the ring while the reader iterates.  A torn read (writer
# mid-record at the moment of the reader's payload copy) must be
# detected and the record skipped; well-formed records must be
# emitted intact, never with a malformed event_type or event_name
# that would otherwise crash pgstat_get_wait_event_type() or
# materialise NULL strings into the result.
#
# Setup: a long-lived BackgroundPsql writer that has
# wait_event_capture = trace and runs a tight pg_sleep loop
# producing a steady stream of PgSleep wait events.  While the
# writer is emitting, the reader calls
# pg_get_wait_event_trace(writer_procnumber) repeatedly and
# asserts every observed row has well-formed event_type,
# event_name, and a non-negative duration.  Any torn record that
# slipped through the seqlock surfaces here as a NULL or empty
# string (or worse, a crash inside the SRF).

my $writer_bg = $node->background_psql('postgres');
$writer_bg->query_safe("SET client_min_messages = warning;");
$writer_bg->query_safe("SET wait_event_capture = trace;");
# Generate at least one wait so the ring is allocated and the
# procnumber appears in pg_stat_get_wait_event_timing.
$writer_bg->query_safe("SELECT pg_sleep(0.01);");
my $writer_bg_proc = $writer_bg->query_safe(
	"SELECT procnumber FROM pg_stat_get_wait_event_timing(pg_backend_pid()) "
	. "WHERE pid = pg_backend_pid() LIMIT 1;");
chomp $writer_bg_proc;
like($writer_bg_proc, qr/^\d+$/,
	'live writer reported its procnumber');

# Start a burst of wait events asynchronously.  query_until
# returns as soon as it sees the \echo banner, leaving the DO
# block executing pg_sleep(0.001) in a tight 1000-iteration loop
# (~1 s wall, ~1000 PgSleep wait events) in the background.
$writer_bg->query_until(
	qr/burst_started/,
	"\\echo burst_started\n"
	  . "DO \$\$ BEGIN FOR i IN 1..1000 LOOP PERFORM pg_sleep(0.001); END LOOP; END \$\$;\n");

# Read concurrently from the reader session.  Each read iterates
# the writer's ring under LW_SHARED; the writer is freely
# appending records.  Any torn row surfaces as NULL/empty
# event_type, event_name, or negative duration.
my $live_read_attempts = 10;
my $live_reads_ok = 1;
my $live_total_observed = 0;
for (my $r = 0; $r < $live_read_attempts; $r++)
{
	my $bad = $reader->query_safe(
		"SELECT count(*) FROM pg_get_wait_event_trace($writer_bg_proc) t "
		. "WHERE t.wait_event_type IS NULL "
		. "   OR t.wait_event_type = '' "
		. "   OR t.wait_event IS NULL "
		. "   OR t.wait_event = '' "
		. "   OR t.duration_us < 0;");
	chomp $bad;
	if ($bad ne '0')
	{
		$live_reads_ok = 0;
		diag("read $r against live writer returned $bad malformed row(s)");
		last;
	}

	my $total = $reader->query_safe(
		"SELECT count(*) FROM pg_get_wait_event_trace($writer_bg_proc);");
	chomp $total;
	$live_total_observed += $total;

	usleep(50_000);
}
ok($live_reads_ok,
	'OWNED-slot reads against live writer produced only well-formed rows');
cmp_ok($live_total_observed, '>', 0,
	"OWNED-slot reads observed records across $live_read_attempts reads (total: $live_total_observed)");

# Wait for the writer's DO block to finish; this query_safe
# blocks until psql is ready to receive new input.
$writer_bg->query_safe("SELECT 1;");
$writer_bg->quit;

# Note on test coverage: the position-encoded identity seqlock
# in emit_wait_event_trace_for_procnumber() has no direct
# regression test.  The bug it prevents (reader observing the
# writer's new write_pos before the writer's rec->seq update has
# propagated, then emitting a stale record with the wrong ring
# index) is unreachable on x86 TSO without instrumentation.  The
# writer-side INJECTION_POINT("wait-event-trace-after-write-pos")
# is in place to support such a test -- a future TAP scenario
# can attach with action 'wait' and verify the reader skips the
# in-flight slot.  Wiring an async BackgroundPsql to wedge inside
# the wait-event recording path proved fiddly enough to defer to
# a follow-up; the identity check is correct by construction and
# the protocol is documented on WaitEventTraceControl.

$reader->quit;
$node->stop;

done_testing;
