# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Wraparound regression test for the wait-event-trace ring buffer.
#
# Provisions a cluster with the smallest legal
# wait_event_trace_ring_size_kb (8 KB = 256 records) and a small
# max_connections, then drives a session through enough wait
# events to force the ring to wrap many times.  Verifies that:
#
#   1. The session-local SRF (pg_get_backend_wait_event_trace)
#      remains queryable when the ring has wrapped: the result is
#      bounded by the ring size, well-formed, and the seq column
#      reflects the most-recent records (not the oldest).
#
#   2. The cross-backend reader (pg_get_wait_event_trace) on a
#      wrapped, currently-OWNED slot also returns well-formed
#      records bounded by the ring size, with the per-record
#      position-encoded identity seqlock correctly distinguishing
#      current-cycle records from overwritten earlier-cycle ones.
#
# If the writer's `pos & ring_mask` indexing or the seqlock's
# identity check (expected_seq = pos*2 + 2) is wrong, this test
# either crashes the reader, produces NULL columns, or returns
# more records than the ring can hold.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('wet_ring_wrap');
$node->init;

# Smallest legal ring size (8 KB = 256 records of 32 bytes each).
# Combined with the loops below this guarantees many ring wraps in
# the writer session.  max_connections kept small so the
# administrative cost of starting/stopping backends stays low.
$node->append_conf(
	'postgresql.conf', q{
max_connections = 20
wait_event_trace_ring_size_kb = 8
});
$node->start;

# Skip if wait-event-timing wasn't compiled in.  Detect via a probe
# SET; the GUC's check hook rejects non-OFF values on stub builds.
my ($rc, $stdout, $stderr) = $node->psql(
	'postgres',
	"SET wait_event_capture = trace;",
	on_error_stop => 0);
if ($stderr =~ /wait event capture is not supported by this build/)
{
	plan skip_all => 'wait-event-timing not compiled in';
}

# Verify the GUC is what we asked for.
my $ring_kb = $node->safe_psql('postgres', "SHOW wait_event_trace_ring_size_kb;");
chomp $ring_kb;
is($ring_kb, '8kB',
	"wait_event_trace_ring_size_kb is the configured value: $ring_kb");

# Drive the writer past many ring wraps.
# Ring = 256 records.  Each pg_sleep(0.0001) emits one wait event
# (the PgSleep latch wait at end).  500 sleeps => roughly 2x the
# ring size (the parse/plan/exec path emits additional waits per
# statement, so the actual ring-wrap factor is higher).
my $writer = $node->background_psql('postgres');
$writer->query_safe("SET client_min_messages = warning;");
$writer->query_safe("SET wait_event_capture = trace;");
$writer->query_safe(
	"DO \$\$ BEGIN FOR i IN 1..500 LOOP PERFORM pg_sleep(0.0001); END LOOP; END \$\$;"
);

my $writer_proc = $writer->query_safe(
	"SELECT procnumber FROM pg_stat_get_wait_event_timing(pg_backend_pid()) "
	. "WHERE pid = pg_backend_pid() LIMIT 1;");
chomp $writer_proc;
like($writer_proc, qr/^\d+$/, 'writer reported its procnumber');

# Session-local read: at most ring-size records, all well-formed,
# seq values reflect the wrapped state (not 0..N-1).
my $local_count = $writer->query_safe(
	"SELECT count(*) FROM pg_get_backend_wait_event_trace();");
chomp $local_count;
cmp_ok($local_count, '<=', 256,
	"session-local read returns at most ring_size records ($local_count <= 256)");
cmp_ok($local_count, '>=', 1,
	"session-local read returns at least one record");

my $local_min_seq = $writer->query_safe(
	"SELECT min(seq) FROM pg_get_backend_wait_event_trace();");
chomp $local_min_seq;
cmp_ok($local_min_seq, '>=', 256,
	"session-local read sees post-wrap seq (min=$local_min_seq >= 256)");

# Cross-backend read of the OWNED slot via pg_get_wait_event_trace.
# This exercises the identity-seqlock check under the wrap regime.
my $reader = $node->background_psql('postgres');

my $cross_count = $reader->query_safe(
	"SELECT count(*) FROM pg_get_wait_event_trace($writer_proc);");
chomp $cross_count;
cmp_ok($cross_count, '<=', 256,
	"cross-backend read returns at most ring_size records ($cross_count <= 256)");
cmp_ok($cross_count, '>=', 1,
	"cross-backend read sees the wrapped ring's records");

my $bad = $reader->query_safe(
	"SELECT count(*) FROM pg_get_wait_event_trace($writer_proc) "
	. "WHERE wait_event_type IS NULL "
	. "   OR wait_event IS NULL "
	. "   OR wait_event_type = '' "
	. "   OR wait_event = '' "
	. "   OR duration_us < 0;");
chomp $bad;
is($bad, '0',
	'cross-backend read after wrap returns only well-formed rows');

# The seq column on the cross-backend SRF reports the writer's
# ring index, which after many wraps should be far above 256.
my $cross_min_seq = $reader->query_safe(
	"SELECT min(seq) FROM pg_get_wait_event_trace($writer_proc);");
chomp $cross_min_seq;
cmp_ok($cross_min_seq, '>=', 256,
	"cross-backend reader sees post-wrap seq (min=$cross_min_seq >= 256)");

$writer->quit;
$reader->quit;
$node->stop;

done_testing;
