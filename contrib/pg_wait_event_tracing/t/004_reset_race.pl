# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_wait_event_tracing: reset race across ProcNumber reuse (fix 5).
#
# A cross-backend reset is published as a generation bump under the
# control lock, but the pid/start-timestamp used to resolve the target
# were captured earlier, outside that lock.  If a successor reuses the
# target's ProcNumber in the window between resolution and taking the
# lock, the request must not land on the successor: pwet_request_reset()
# re-checks owner_pid/owner_start against the resolved target under the
# same lock that publishes the bump, so a mismatch (successor already
# attached) leaves the slot alone.
#
# This test forces exactly that window open with the
# "pg-wait-event-tracing-reset-before-publish" injection point (placed
# between resolution and taking the lock -- see pg_wait_event_tracing.c),
# swaps in a successor while the requester is parked there, and checks
# the successor's own counters and reset_count come out untouched.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'Injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_wait_event_tracing, injection_points'");
# Keep pg_sleep() running in the session that issued it, not a parallel
# worker, so its wait is recorded under the pid this test is watching.
$node->append_conf('postgresql.conf', "debug_parallel_query = off");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION pg_wait_event_tracing;');
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $point = 'pg-wait-event-tracing-reset-before-publish';

# A: one recorded wait, then note its pid and ProcNumber.
my $A = $node->background_psql('postgres');
$A->query_safe("SET pg_wait_event_tracing.capture = stats;");
$A->query_safe("SELECT pg_sleep(0.01);");
my $a_pid = $A->query_safe("SELECT pg_backend_pid();");
my $a_procnumber = $node->safe_psql(
	'postgres',
	"SELECT procnumber FROM pg_stat_wait_event_timing "
	  . "WHERE pid = $a_pid AND wait_event = 'PgSleep';");

# R: a superuser session that attaches the injection point and then
# starts a reset of A's session, which will block right before
# publishing the request.
my $R = $node->background_psql('postgres');
$R->query_safe("SELECT injection_points_attach('$point', 'wait');");
$R->query_until(
	qr/reset_launched/,
	"\\echo reset_launched\n"
	  . "SELECT pg_stat_reset_wait_event_timing($a_pid);\n");

$node->wait_for_event('client backend', $point);

# While R is parked at the injection point, replace A with B: quit A and
# connect B right away, so the PGPROC free list's LIFO order hands B
# A's now-vacant ProcNumber.
$A->quit;
$node->poll_query_until('postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $a_pid);"
) or die "backend $a_pid did not disappear from pg_stat_activity";

my $B = $node->background_psql('postgres');
$B->query_safe("SET pg_wait_event_tracing.capture = stats;");
$B->query_safe("SELECT pg_sleep(0.01);");
$B->query_safe("SELECT pg_sleep(0.01);");
my $b_pid = $B->query_safe("SELECT pg_backend_pid();");
my $b_procnumber = $node->safe_psql(
	'postgres',
	"SELECT procnumber FROM pg_stat_wait_event_timing "
	  . "WHERE pid = $b_pid AND wait_event = 'PgSleep';");

# Now let R's stale request through, regardless of whether the reuse
# below is confirmed: R must not be left blocked at the injection point
# through the rest of the test (or its teardown).  It targeted A's old
# owner token, which B's attach has since overwritten, so it must not
# touch B's slot.
$node->safe_psql('postgres', "SELECT injection_points_wakeup('$point');");
$R->quit;

# One more wait after the release, so a wrongly-applied reset (which
# would only be noticed at the *next* wait_end -- see
# t/003_reset_acl.pl) has every opportunity to show up here too.
$B->query_safe("SELECT pg_sleep(0.01);");

SKIP:
{
	skip "ProcNumber $a_procnumber was not reused by B (B got "
	  . "$b_procnumber instead); cannot exercise the race in this run", 2
	  unless $b_procnumber eq $a_procnumber;

	is( $node->safe_psql(
			'postgres',
			"SELECT calls FROM pg_stat_wait_event_timing "
			  . "WHERE pid = $b_pid AND wait_event = 'PgSleep';"
		),
		'3',
		"B's PgSleep count reflects all three of its own waits");
	is( $node->safe_psql(
			'postgres',
			"SELECT reset_count FROM pg_stat_wait_event_timing_overflow "
			  . "WHERE pid = $b_pid;"
		),
		'0',
		"the reset aimed at A's stale token was not consumed by B");
}

$node->safe_psql('postgres', "SELECT injection_points_detach('$point');");

$B->quit;

done_testing();
