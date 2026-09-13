# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_wait_event_tracing: two marker-set corners the module's own regress
# test (sql/pg_wait_event_tracing_trace.sql) deliberately does not cover,
# because a deterministic .sql script cannot produce either hazard on
# demand:
#
#   (a) the Idle marker.  pwet_wait_begin() only synthesizes it when a
#       ClientRead wait actually blocks -- and secure_read() (be-secure.c)
#       only reports WAIT_EVENT_CLIENT_READ from the branch taken after a
#       non-blocking read returns EWOULDBLOCK, never around a read that is
#       immediately satisfied from already-buffered client bytes.  Whether
#       that happens for two statements sent from a .sql file depends on
#       loaded-runner scheduling, not protocol structure, so the regress
#       test filters Idle out of every case entirely and documents this
#       exact deferral.  A TAP test controls the client side directly: a
#       real pause between two statements must produce an Idle marker
#       between them.
#
#       The opposite check -- two statements that share ONE simple-query
#       protocol message never see an Idle between them -- needs an
#       actual single message, not just two statements on one input
#       line: CI showed that psql, reading a script from a file (or
#       $node->safe_psql's string), sends each ;-terminated statement as
#       its own message regardless of shared line placement, so "two
#       statements, one line" was exactly as timing-dependent as the
#       thing being tested, and failed on Windows/macOS while passing on
#       Linux. `psql -c 'SELECT 1; SELECT 2;'` does send the whole
#       string as one message, so that is what this case uses; see its
#       own comment below for how the read-back is made independent of
#       that mechanism's other, unavoidable message boundaries (the
#       preceding SET and the session's own exit).
#
#   (b) pwet_marker_txn_abort()'s defensive pwet_exec_depth reset.  The
#       regress test's own error case (SELECT 1/0) raises at PLANNING
#       time -- eval_const_expressions() folds the constant division
#       before ExecutorStart is ever reached -- so pwet_exec_depth never
#       moves and the reset is never exercised.  Here, a PL/pgSQL PERFORM
#       divides by a column value that is only zero on the second row of
#       a table scan, so the division-by-zero can only be discovered
#       while genuinely executing that nested statement.  The call is
#       wrapped in a PROCEDURE, not a plain function: CALL is dispatched
#       through ProcessUtility (T_CallStmt in standard_ProcessUtility),
#       not the executor, so the outer call contributes a UtilityStart
#       marker without ever touching pwet_exec_depth, and exactly ONE
#       nested executor level -- the PERFORM's own -- is left unclosed by
#       the error.  Without the reset, every later statement's own
#       ExecStart would be off by exactly that one level.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', q(
shared_preload_libraries = 'pg_wait_event_tracing'
debug_parallel_query = off
));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION pg_wait_event_tracing;');

# ---------------------------------------------------------------------
# (a) Idle marker: present after a real client-side pause between two
# statements, absent between two statements sent as one simple-query
# message.
# ---------------------------------------------------------------------
my $psql = $node->background_psql('postgres');
$psql->query_safe('SET pg_wait_event_tracing.capture = trace;');

my $mark_a = $psql->query_safe(
	"SELECT coalesce(max(seq), -1) FROM pg_backend_wait_event_trace;");
$psql->query_safe('SELECT 1;');

# A real, generous client-side pause: by the time this session next
# tries to read the following message, nothing has arrived yet, so
# secure_read() genuinely blocks in WaitEventSetWait(WAIT_EVENT_CLIENT_
# READ) -- the only place the Idle marker is synthesized (see the file
# header).  A couple of seconds is comfortably more than any scheduling
# jitter on a loaded CI runner needs to be sure of that.
sleep(2);
$psql->query_safe('SELECT 2;');

my $markers_a = $psql->query_safe(
	"SELECT string_agg(wait_event, ',' ORDER BY seq) "
	  . "FROM pg_backend_wait_event_trace "
	  . "WHERE wait_event_type = 'Query' AND seq > $mark_a;");
like($markers_a, qr/(^|,)Idle(,|$)/,
	'a real client-side pause between two statements produces an Idle marker'
);

$psql->quit;

# psql's -c switch sends its whole argument as ONE simple-query protocol
# message, so "SELECT 1; SELECT 2;" below genuinely cannot see a
# ClientRead wait -- and so no Idle -- in between: the backend does not
# attempt to read again until it has processed the entire message.  The
# pid/procnumber lookups run BEFORE capture is enabled, so they add no
# markers to this session's ring at all (pwet_trace_write_marker() is a
# no-op outside capture = trace) -- which makes the very first
# 'QueryStart' this ring ever records unambiguously the -c action's own
# (the SET action right before it only ever produces UtilityEnd/
# TxnCommit, never QueryStart).  That "first QueryStart" anchor is what
# makes the read-back immune to the OTHER, unavoidable message
# boundaries this design still has: a real ClientRead wait (and an Idle
# marker) between the SET action and the -c action lands strictly
# BEFORE the anchor, and one between the -c action and the session's own
# exit lands strictly AFTER the fixed eight-marker window read from that
# anchor, so neither can be mistaken for something between the two
# target statements.  The session exits after the -c action, orphaning
# its ring (same mechanism as t/005_orphan_reuse.pl), which is read back
# cross-backend once the backend is confirmed gone.
my $setup_sql = "SELECT pg_backend_pid();\n"
  . "SELECT id FROM pg_stat_get_backend_idset() AS id "
  . "WHERE pg_stat_get_backend_pid(id) = pg_backend_pid();\n"
  . "SET pg_wait_event_tracing.capture = trace;\n";
my (undef, $case2_out, undef) = $node->psql(
	'postgres', $setup_sql,
	on_error_die => 1,
	extra_params => [ '-c', 'SELECT 1; SELECT 2;' ]);
my ($case2_pid, $case2_procnumber) = split /\n/, $case2_out;

$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $case2_pid);"
) or die "backend $case2_pid did not disappear from pg_stat_activity";

my $markers_b = $node->safe_psql(
	'postgres', qq(
	WITH m AS (
	    SELECT seq, wait_event,
	           row_number() OVER (ORDER BY seq) AS rn
	    FROM pg_get_wait_event_trace($case2_procnumber)
	    WHERE wait_event_type = 'Query'
	),
	anchor AS (
	    SELECT min(rn) AS start_rn FROM m WHERE wait_event = 'QueryStart'
	)
	SELECT string_agg(m.wait_event, ',' ORDER BY m.seq)
	FROM m, anchor
	WHERE m.rn >= anchor.start_rn AND m.rn < anchor.start_rn + 8;
));
is( $markers_b,
	'QueryStart,ExecStart,ExecEnd,TxnCommit,QueryStart,ExecStart,ExecEnd,TxnCommit',
	'two statements sent as one simple-query message produce exactly '
	  . 'their own eight markers, with no Idle between them');

# ---------------------------------------------------------------------
# (b) pwet_marker_txn_abort()'s defensive pwet_exec_depth reset, after an
# error raised during execution (not planning), inside a nested call.
# ---------------------------------------------------------------------
$node->safe_psql(
	'postgres', q(
CREATE TABLE pwet_trace_divzero_rows (d int);
INSERT INTO pwet_trace_divzero_rows VALUES (1), (0);
CREATE PROCEDURE pwet_trace_test_divzero() LANGUAGE plpgsql AS $body$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT d FROM pwet_trace_divzero_rows ORDER BY d DESC LOOP
        PERFORM 1 / r.d;
    END LOOP;
END
$body$;
));

# on_error_stop => 0: the CALL below is expected to fail, and the same
# session must survive it to run a following statement.  A check that
# expects an ERROR must not use a plain background_psql session (its
# default on_error_stop would make psql exit on the error, and the next
# call into this session would die with "process ended prematurely").
my $psql2 = $node->background_psql('postgres', on_error_stop => 0);
$psql2->query_safe('SET pg_wait_event_tracing.capture = trace;');

$psql2->query('CALL pwet_trace_test_divzero();');
like($psql2->{stderr}, qr/division by zero/,
	'the PERFORM divides by zero on the second row, mid-execution');
$psql2->{stderr} = '';

# The next, ordinary statement's own ExecStart marker is self-
# referential (same as the regress test: post_parse_analyze/
# ExecutorStart write this SELECT's own QueryStart/ExecStart before its
# body runs), so its depth field reports the nesting level in effect
# right after the abort.  Without pwet_marker_txn_abort()'s reset, the
# PERFORM's own ExecStart -- never matched by an ExecEnd, since the
# error struck mid-execution -- would leave pwet_exec_depth stuck at 1
# forever.
my $depth = $psql2->query_safe(
	"SELECT depth FROM pg_backend_wait_event_trace "
	  . "WHERE wait_event = 'ExecStart' ORDER BY seq DESC LIMIT 1;");
is($depth, '0',
	"a normal statement's ExecStart depth is back at 0 after the aborted CALL"
);

$psql2->quit;
$node->stop;

done_testing();
