# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_wait_event_tracing: reset authorization (fix 4).
#
# pg_stat_reset_wait_event_timing(pid) replicates pg_signal_backend()'s
# target-authorization rule: a non-superuser cannot touch a
# superuser-owned or role-less target, and otherwise needs privileges of
# the target role or of pg_signal_backend.  Resetting one's own backend
# (NULL or its own pid) always succeeds and is synchronous.  A
# cross-backend reset is asynchronous: it only bumps a generation
# counter, and the target backend clears its own counters the next time
# it goes through wait_end(), so this test always drives the target
# through one more wait after a successful cross-backend reset before
# checking that its counters were cleared.  pg_stat_reset_wait_event_timing_all()
# is superuser-only in C, independent of any EXECUTE grant.
#
# This exercises the cross-backend cases that the module's own regress
# test cannot: it has no second connection, and resetting one's own pid
# always takes the synchronous self-reset path regardless of role, so
# none of the authorization branches below are reachable from a single
# session.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(auth_extra => [
		'--create-role',
		'regress_a,regress_a2,regress_b,regress_sig,regress_su'
	]);
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_wait_event_tracing'");
# Keep pg_sleep() running in the session that issued it, not a parallel
# worker, so its wait is recorded under the pid this test is watching.
$node->append_conf('postgresql.conf', "debug_parallel_query = off");
$node->start;
$node->safe_psql(
	'postgres', q(
CREATE EXTENSION pg_wait_event_tracing;
CREATE ROLE regress_su LOGIN SUPERUSER;
CREATE ROLE regress_a LOGIN;
CREATE ROLE regress_a2 LOGIN IN ROLE regress_a;
CREATE ROLE regress_b LOGIN;
CREATE ROLE regress_sig LOGIN IN ROLE pg_signal_backend;
));

sub connect_as
{
	my ($role) = @_;
	return $node->background_psql('postgres',
		connstr => $node->connstr('postgres') . " user=$role");
}

sub pgsleep_calls
{
	my ($pid) = @_;
	return $node->safe_psql(
		'postgres',
		"SELECT coalesce((SELECT calls FROM pg_stat_wait_event_timing "
		  . "WHERE pid = $pid AND wait_event = 'PgSleep'), 0);");
}

sub reset_count
{
	my ($pid) = @_;
	return $node->safe_psql('postgres',
		"SELECT reset_count FROM pg_stat_wait_event_timing_overflow "
		  . "WHERE pid = $pid;");
}

# reset_as() issues a reset as $role against $target_pid on a one-shot
# connection: the request itself is a quick, synchronous lock-protected
# generation bump (no injection point involved here, unlike
# t/004_reset_race.pl), so there is no need to keep the actor's session
# alive.
sub reset_as
{
	my ($role, $target_pid) = @_;
	return $node->psql(
		'postgres',
		"SELECT pg_stat_reset_wait_event_timing($target_pid);",
		connstr => $node->connstr('postgres') . " user=$role");
}

# Three live targets, each with capture enabled and one recorded wait.
my $SU = connect_as('regress_su');
$SU->query_safe("SET pg_wait_event_tracing.capture = stats;");
$SU->query_safe("SELECT pg_sleep(0.01);");
my $su_pid = $SU->query_safe("SELECT pg_backend_pid();");

my $A = connect_as('regress_a');
$A->query_safe("SET pg_wait_event_tracing.capture = stats;");
$A->query_safe("SELECT pg_sleep(0.01);");
my $a_pid = $A->query_safe("SELECT pg_backend_pid();");

my $B = connect_as('regress_b');
$B->query_safe("SET pg_wait_event_tracing.capture = stats;");
$B->query_safe("SELECT pg_sleep(0.01);");
my $b_pid = $B->query_safe("SELECT pg_backend_pid();");

is(pgsleep_calls($su_pid), '1', "fixture: SU has one recorded wait");
is(pgsleep_calls($a_pid),  '1', "fixture: A has one recorded wait");
is(pgsleep_calls($b_pid),  '1', "fixture: B has one recorded wait");

###
# own reset (NULL and own pid) by regress_b: succeeds, synchronously.
###
my $rc = reset_count($b_pid);
$B->query_safe("SELECT pg_stat_reset_wait_event_timing(NULL);");
is(pgsleep_calls($b_pid), '0', "B's own NULL-reset clears its own counters");
is(reset_count($b_pid), $rc + 1, "B's own NULL-reset bumps its reset_count");

$B->query_safe("SELECT pg_sleep(0.01);");
$rc = reset_count($b_pid);
$B->query_safe("SELECT pg_stat_reset_wait_event_timing($b_pid);");
is(pgsleep_calls($b_pid), '0',
	"B's own-pid reset also clears its own counters");
is(reset_count($b_pid), $rc + 1, "B's own-pid reset bumps its reset_count");

# Leave B primed with one wait for the regress_sig case below.
$B->query_safe("SELECT pg_sleep(0.01);");

###
# regress_a2 resets regress_a's session: succeeds (a2 is a member of a).
###
$rc = reset_count($a_pid);
my ($ret, $stdout, $stderr) = reset_as('regress_a2', $a_pid);
is($ret, 0, "regress_a2 can reset regress_a's session");
is($stderr, '', "...with no error output");

$A->query_safe("SELECT pg_sleep(0.01);");
is(pgsleep_calls($a_pid), '1',
	"A's counters were cleared before this wait was recorded");
is(reset_count($a_pid), $rc + 1, "A's reset_count reflects the cross-backend reset");

###
# regress_sig resets regress_b's session: succeeds (sig is a member of
# pg_signal_backend).
###
$rc = reset_count($b_pid);
($ret, $stdout, $stderr) = reset_as('regress_sig', $b_pid);
is($ret, 0, "regress_sig can reset regress_b's session");
is($stderr, '', "...with no error output");

$B->query_safe("SELECT pg_sleep(0.01);");
is(pgsleep_calls($b_pid), '1',
	"B's counters were cleared before this wait was recorded");
is(reset_count($b_pid), $rc + 1, "B's reset_count reflects the cross-backend reset");

###
# regress_b resets regress_su's session: permission denied (regress_b is
# neither superuser nor a member of pg_signal_backend, and the target is
# superuser-owned).
###
($ret, $stdout, $stderr) = reset_as('regress_b', $su_pid);
isnt($ret, 0, "regress_b cannot reset regress_su's session");
like($stderr, qr/permission denied/, "...permission denied error");
is(pgsleep_calls($su_pid), '1', "SU's counters are untouched by the failed attempt");

###
# regress_b resets regress_a's session: permission denied (regress_b has
# privileges of neither regress_a nor pg_signal_backend).
###
($ret, $stdout, $stderr) = reset_as('regress_b', $a_pid);
isnt($ret, 0, "regress_b cannot reset regress_a's session");
like($stderr, qr/permission denied/, "...permission denied error");
is(pgsleep_calls($a_pid), '1', "A's counters are untouched by the failed attempt");

###
# Any role resetting the checkpointer's pid gets pg_signal_backend()'s
# own WARNING wording, not an error: BackendPidGetProc() only resolves
# normal backends, so auxiliary pids are rejected before any ACL check
# even runs.
###
my $checkpointer_pid = $node->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer';"
);
($ret, $stdout, $stderr) = reset_as('regress_b', $checkpointer_pid);
is($ret, 0, "resetting the checkpointer's pid is not an error");
like($stderr, qr/is not a PostgreSQL backend process/,
	"...but does warn that it is not a backend");

###
# regress_b calling pg_stat_reset_wait_event_timing_all() is an error:
# it hard-requires superuser() in C, regardless of any EXECUTE grant.
###
($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	"SELECT pg_stat_reset_wait_event_timing_all();",
	connstr => $node->connstr('postgres') . ' user=regress_b');
isnt($ret, 0, "regress_b cannot call the _all() reset");
like($stderr, qr/permission denied/, "...permission denied error");

$SU->quit;
$A->quit;
$B->quit;

done_testing();
