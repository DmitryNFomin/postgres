# W6c power-state diagnostic

This package diagnoses the cell-to-cell throughput plateaus seen in the
interrupted r3 run. It does not measure v9 or v10 overhead and must not be
used as final benchmark evidence.

The diagnostic reuses the already verified r3 baseline A/B builds. It runs
eight randomized A/B pairs (16 W6c cells) with exactly the r3 workload,
dataset setup, PostgreSQL mask `1-63:2`, and pgbench mask `0-14:2`.

One-second evidence includes:

- `turbostat --Summary`;
- all per-CPU `scaling_cur_freq` values;
- per-CPU utilization, interrupts, and softirqs;
- RAPL energy counters when exposed;
- temperature sensors when exposed;
- NUMA counters and per-process `numa_maps`;
- block-device counters;
- pgbench progress and PostgreSQL logs.

## Safety

- Run the wrapper as root, but PostgreSQL, `initdb`, and pgbench run as the
  original non-root executor.
- Turbo must be enabled and remains unchanged.
- The package does not write sysfs, MSRs, governor settings, or turbo state.
- Existing partial r3 results are not read, modified, resumed, or removed.
- A running PostgreSQL server or load average above 0.5 blocks startup.

## Run

Copy the archive and checksum to the server. Compare the archive digest with
the SHA-256 value in the delivery message, not only with the adjacent
checksum file. Then, as root, copy and extract it under a new root-controlled
directory:

```sh
RUN_DIR=$(mktemp -d /root/w6c-power-diag.XXXXXX)
cp /home/dmitry/wet-v10-w6c-power-diag-r2.tar.gz* "$RUN_DIR/"
cd "$RUN_DIR"
sha256sum wet-v10-w6c-power-diag-r2.tar.gz
sha256sum -c wet-v10-w6c-power-diag-r2.tar.gz.sha256
tar --no-same-owner -xzf wet-v10-w6c-power-diag-r2.tar.gz
cd wet-v10-w6c-power-diag-r2
./run-root.sh /home/dmitry/wet-v10-baremetal-r3
```

Expected duration is approximately 18–25 minutes. On success, return:

```text
/var/tmp/w6c-power-diag.<random>/w6c-power-diag-<UTC>.tar.gz
/var/tmp/w6c-power-diag.<random>/w6c-power-diag-<UTC>.tar.gz.sha256
```

The output directory is root-owned, but the two completed files belong to
the original non-root executor. If the run stops, preserve the staging path
printed by the wrapper and return `driver.log`.
