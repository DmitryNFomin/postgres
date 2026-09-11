# Copyright (c) 2026, PostgreSQL Global Development Group

# pg_wait_event_tracing: ownership across ProcNumber reuse (fix 2).
#
# PGPROC's free list is LIFO, so a new backend that connects right after
# another one exits -- with nothing else connecting in between -- reuses
# the exiting backend's ProcNumber.  The always-resident control slot for
# that ProcNumber records owner_pid/owner_start alongside the DSA payload
# pointer, and every reader compares them against the live
# PgBackendStatus entry before trusting the payload, so a successor never
# gets attributed a predecessor's counters, even before it has attached
# its own payload.  This test drives that reuse deliberately and checks
# both readers -- a superuser, and the backend reading about itself --
# see the right thing at each step.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(auth_extra => ['--create-role', 'regress_a,regress_b']);
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_wait_event_tracing'");
# Keep pg_sleep() running in the session that issued it, not a parallel
# worker, so its wait is recorded under the pid this test is watching.
$node->append_conf('postgresql.conf', "debug_parallel_query = off");
$node->start;
$node->safe_psql(
	'postgres', q(
CREATE EXTENSION pg_wait_event_tracing;
CREATE ROLE regress_a LOGIN;
CREATE ROLE regress_b LOGIN;
));

# Session A: one recorded wait, then note its pid and ProcNumber.
my $A = $node->background_psql(
	'postgres',
	connstr => $node->connstr('postgres') . ' user=regress_a');
$A->query_safe("SET pg_wait_event_tracing.capture = stats;");
$A->query_safe("SELECT pg_sleep(0.01);");
my $a_pid = $A->query_safe("SELECT pg_backend_pid();");
my $a_procnumber = $node->safe_psql(
	'postgres',
	"SELECT procnumber FROM pg_stat_wait_event_timing "
	  . "WHERE pid = $a_pid AND wait_event = 'PgSleep';");

$A->quit;
$node->poll_query_until(
	'postgres',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $a_pid);"
) or die "backend $a_pid did not disappear from pg_stat_activity";

# Connect B right away, before anything else can grab A's freed
# ProcNumber.  B has not enabled capture yet.
my $B = $node->background_psql(
	'postgres',
	connstr => $node->connstr('postgres') . ' user=regress_b');
my $b_pid = $B->query_safe("SELECT pg_backend_pid();");

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_wait_event_timing WHERE pid = $b_pid;"
	),
	'0',
	"B has no timing rows before enabling capture");
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_wait_event_timing_overflow "
		  . "WHERE pid = $b_pid;"
	),
	'0',
	"B has no overflow rows before enabling capture");

# Now B enables capture and records its own wait.
$B->query_safe("SET pg_wait_event_tracing.capture = stats;");
$B->query_safe("SELECT pg_sleep(0.01);");
my $b_procnumber = $node->safe_psql(
	'postgres',
	"SELECT procnumber FROM pg_stat_wait_event_timing "
	  . "WHERE pid = $b_pid AND wait_event = 'PgSleep';");

SKIP:
{
	skip "ProcNumber $a_procnumber was not reused by B (B got "
	  . "$b_procnumber instead); cannot exercise the reuse path in this run",
	  3
	  unless $b_procnumber eq $a_procnumber;

	is($b_procnumber, $a_procnumber,
		"B reused A's ProcNumber, exactly what the ownership check guards"
	);

	# B's own counters, not A's: a fresh count of 1, read as the
	# superuser reader.
	is( $node->safe_psql(
			'postgres',
			"SELECT calls FROM pg_stat_wait_event_timing "
			  . "WHERE pid = $b_pid AND wait_event = 'PgSleep';"
		),
		'1',
		"superuser reader sees B's own fresh count, not A's leftover data"
	);

	# Same data, read by B itself.  The view is revoked from PUBLIC, so
	# this exercises the underlying function directly, which relies on
	# the self-privilege branch of the internal check rather than a
	# granted view or pg_read_all_stats membership.
	is( $B->query_safe(
			"SELECT calls FROM pg_stat_get_wait_event_timing(pg_backend_pid()) "
			  . "WHERE wait_event = 'PgSleep';"
		),
		'1',
		"B can read its own row via the function despite no view grant"
	);
}

# The view itself stays off limits to a role with no pg_read_all_stats,
# unlike the function form used above.
$B->{stderr} = '';
$B->query("SELECT * FROM pg_stat_wait_event_timing;");
like($B->{stderr}, qr/permission denied/,
	"B cannot read the view directly, only the function about itself");

$B->quit;

done_testing();
