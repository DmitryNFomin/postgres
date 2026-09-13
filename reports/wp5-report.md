# WP5 report: contrib documentation, xfunc.sgml, removal audit

Branch `wet-v8-wp5`, base `wet-v8-wp3` (per the coordinator's override, not
`wet-v7-rfc` as the brief's own D3 base-comparison line said — D3's audit
below still diffs against `wet-v7-rfc`, since that is the pre-series base
the whole module must be scoped against, independent of which WP tip the
docs were written from). Worktree
`<workspace>/wt-v8-wp5`. The main checkout at
`<workspace>/postgres` stayed on `wet-series`
throughout; nothing was pushed; no other worktree was touched.

Read wp2-report.md, wp2b-report.md, wp3-report.md, the extension script
(`contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql`), and the
module's C source and header
(`contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`,
`pg_wait_event_tracing_data.h`) directly, plus the actual regression test
files (`sql`/`expected/pg_wait_event_tracing_trace.{sql,out}`) for the
literal marker sequences, before writing anything — per the brief's own
instruction, the docs describe what this branch's code does, not what v6
or the plan said it would do.

## Commits

| # | Commit | Subject |
|---|---|---|
| D1 | `03dd07b9d65` | pg_wait_event_tracing: add contrib module documentation |
| D2 | `38fe0ae854d` | doc: document the timed wait-event reporting pair for add-ins |

Both authored `Dmitry Fomin <fomin.list@gmail.com>`, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.

- **D1** adds `doc/src/sgml/pgwaiteventtracing.sgml` (1516 lines) and
  registers it in `contrib.sgml` (alphabetically, between `&pgvisibility;`
  and `&pgwalinspect;`) and `filelist.sgml`.
- **D2** edits `doc/src/sgml/xfunc.sgml`: adds one paragraph to the
  "Custom Wait Events" section explaining
  `pgstat_report_wait_start_timed()`/`pgstat_report_wait_end_timed()`, and
  switches the file's one actual code example of the ordinary pair (the
  injection-point callback example, in the neighboring "Injection Points"
  section — see "Where the code example lives" below) to the timed pair
  under `#if PG_VERSION_NUM >= 200000`, falling back to the ordinary pair.

No D3 commit: the audit found nothing inside the module needing a code
fix (see below).

## D1 — `pgwaiteventtracing.sgml` content, section by section

1. Intro: what the module is (exact wait-event timing, not sampling, via
   the timed hook pair) and the three levels (`off`/`stats`/`trace`).
2. Loading: `shared_preload_libraries`, the `_PG_init()` error wording,
   cluster-wide collection independent of which database has the
   extension, and the three GUCs
   (`pg_wait_event_tracing.capture` enum off/stats/trace, `PGC_SUSET`,
   default off; `.max_tranches` integer, `PGC_POSTMASTER`, default 192,
   min 16, max 65534; `.trace_ring_size` integer KB,
   `PGC_POSTMASTER`, default 4096, min 8, max 32768, power-of-two check).
3. Views and functions: every column of
   `pg_stat_wait_event_timing`/`_overflow`,
   `pg_wait_event_timing_histogram_buckets` (full 32-row bucket table,
   transcribed from the script's own `VALUES` list),
   `pg_wait_event_tracing_capacity()` (with the actual current capacity
   numbers: Lock 32, Buffer 16, Activity 32, Client 32, Extension 128,
   IPC 128, Timeout 32, IO 128, InjectionPoint 32),
   `pg_backend_wait_event_trace`/`pg_get_wait_event_trace()`,
   `pg_wait_event_trace_by_statement()`, and the three non-view functions
   — each with its type, one-line meaning, and its actual grant state
   read from the script (including the PUBLIC-executable-but-per-row-
   filtered asymmetry between the stats functions and the
   pg_read_all_stats-only trace functions, and that
   `pg_wait_event_timing_histogram_buckets`/`pg_wait_event_tracing_capacity()`
   carry no explicit grant at all — see "Disagreements" below).
4. Statistics semantics: per-live-backend rows, parallel workers excluded
   (separate backend/row, `backend_type` "parallel worker"), which call
   sites are timed (essentially the whole tree — see "the timed-site
   list" below), the 32-bit torn-read caveat, and a dedicated subsection
   on server-side/auxiliary processes covering exactly the behaviour in
   override 2: fixed-memory reservation from process start when capture
   is non-off at postmaster start (~10 MiB at the example defaults, zero
   otherwise), attach-at-next-reload if turned on later, trace never
   covered by the reservation (attaches only at the first reload with
   `capture = trace`), and crash recovery therefore visible in stats but
   never in trace.
5. Memory: control table (~10 KB at the example defaults, always
   resident), per-backend stats payload (212,664 bytes at the default
   `max_tranches = 192`, from wp3's own verified figure), the ~10.1 MiB
   server-process reservation as a multiple of that same per-payload
   size, and the trace ring (4 MiB at the default), each stated as
   proportional only to backends that actually enable that level, never
   to `max_connections`.
6. Resetting and permissions: own reset, the `pg_signal_backend()`-style
   cross-backend rule (including the no-autovacuum-carve-out deviation
   and the "auxiliary pid rejected, not erroring" behaviour), asynchronous
   application at the target's next wait, `_all()`'s hard superuser-in-C
   requirement.
7. Trace ring and post-mortem reading: the orphan contract stated before
   any example, as required (survives an orderly exit until reclaimed or
   swept; does not survive a whole-cluster crash restart, since that
   recreates all of shared memory), then `pg_get_wait_event_trace()`
   usage and the sweep function.
8. Query markers and statement attribution (the longest section): the
   full marker table with exact emission sites and `record_type`
   meanings; the state machine; the attribution rule
   (`pg_wait_event_trace_by_statement()`'s boundary markers, `<idle>`,
   `<unattributed>`); six worked timelines taken **verbatim** from
   `expected/pg_wait_event_tracing_trace.out` (autocommit statement,
   explicit two-statement transaction, same-line multi-statement string,
   utility statement, planning-time error, one-level nested call with
   depths), plus two illustrative (not regress-tested, and labeled as
   such) timelines for a mid-execution error and a pipelined
   extended-protocol batch, derived directly from the hook-placement
   comments in the C code; and the five-item limitations list exactly as
   specified in override 2 (no pipelined end-of-message marker; parallel
   workers under their own backend; unattributed pre-first-marker waits;
   `query_id` 0 without `compute_query_id`; `ClientRead` inside `COPY
   FROM STDIN` belongs to the COPY).
9. Reading from other extensions: SQL functions only, no direct
   shared-memory reader exported (fix 7).
10. Overhead: the required `<!-- COST NUMBERS: filled in after the
    bare-metal re-measurement -->` placeholder, with the
    off/loaded-but-off/stats/trace structure described qualitatively and
    no numbers.

### The timed-site list

For "which call sites are timed" I did not rely on the plan's wording
alone: I grepped the whole tree for
`pgstat_report_wait_start_timed`/`_end_timed` call sites and found 34
files use the timed pair (essentially every core wait-reporting site:
`lwlock.c`/`s_lock.c` for every LWLock/spinlock wait, `waiteventset.c`
for `ClientRead` and friends, all the WAL/replication/vacuum/I/O files,
and every auxiliary process's own main loop), against only 2 files still
using the plain, non-timed pair (`wait_event.c`, the API's own definition
site, and `src/test/modules/injection_points/injection_points.c`, a test
module). The docs describe this honestly as "essentially universal
coverage" rather than listing all 34 files.

## D2 — where the code example lives

The brief says "in the custom wait events section: ... switch the
example to the timed pair." I checked: the "Custom Wait Events" sect2
(`xfunc-addin-wait-events`) itself contains no code example using
`pgstat_report_wait_start()`/`pgstat_report_wait_end()` at all — only a
call to `WaitEventExtensionNew()` and a `pg_stat_activity` query. The
**only** place in the entire file (grepped) that shows the ordinary pair
in use is the injection-point callback example in the immediately
following sect2 (`xfunc-addin-injection-points`). I added the explanatory
paragraph to the Custom Wait Events section as instructed, and applied
the actual code switch to that one example, since it is unambiguously
the "the example" the instruction means. Flagging this reading for the
reviewer in case a different placement was intended.

## D3 — removal audit

**Path scope**, diffed against `wet-v7-rfc` (the pre-series base) at the
final `wet-v8-wp5` tip:

```
contrib/Makefile                                     |   1 +
contrib/meson.build                                  |   1 +
contrib/pg_wait_event_tracing/**                     |  ... (all module files)
doc/src/sgml/contrib.sgml                            |   1 +
doc/src/sgml/filelist.sgml                           |   1 +
doc/src/sgml/pgwaiteventtracing.sgml                 | 1516 ++
doc/src/sgml/xfunc.sgml                              |  31 +-
18 files changed, 6045 insertions(+), 1 deletion(-)
```

No file outside `contrib/pg_wait_event_tracing/**`, `contrib/Makefile`,
`contrib/meson.build`, and `doc/src/sgml/{pgwaiteventtracing,contrib,
filelist,xfunc}.sgml` is touched. `src/test/modules/test_wait_hook/**`
and `src/test/modules/{Makefile,meson.build}` are WP1's, already merged
into `wet-v8-wp3`'s ancestry (via `wet-v7-rfc`) before this WP started, so
they don't appear in this WP's own diff against that base, but the
combined branch still only touches the allowed set overall (confirmed:
`git diff --name-only wet-v7-rfc..HEAD` filtered against the full allowed
pattern list from the brief, including the test_wait_hook paths, returns
nothing outside it).

**Grep for v6 leftovers**, whole tree, zero hits for: `wait_event_capture`
(unprefixed), `wait_event_timing_max_tranches`, `--enable-wait-event-timing`,
`USE_WAIT_EVENT_TIMING`, `wait_event_timing.h`, `WaitEventTraceCtl`,
`pgstat_set_wait_event_timing_storage`, and OIDs 9956–9962 (checked both as
bare numbers near "wait_event" and in `pg_proc.dat`-style catalog
entries).

One hit for `wait_event_trace_ring_size` (unprefixed): a comment in
`pg_wait_event_tracing.c` line 354 — "Per-backend trace ring size in KB
(same default/min/max/unit as v6's `wait_event_trace_ring_size`)" — a
deliberate lineage comment contrasting the old name with this module's
own prefixed GUC, not a leftover reference to the old symbol itself. I
did not touch it (D3 is report-only unless something needs fixing, and
this is correct as written).

**Demo scripts** (`blog/demo/run_demo*.sh`), old GUC names found, not
edited per the brief:

- `run_demo.sh:82` — `run_sql "SHOW wait_event_capture; SHOW compute_query_id;"`
- `run_demo.sh:201` — `PGOPTIONS='-c wait_event_capture=trace'`
- `run_demo_trace.sh:68` — `'wait_event_capture=stats'`
- `run_demo_trace.sh:103` — `SHOW wait_event_capture;`
- `run_demo_trace.sh:105` — `SHOW wait_event_trace_ring_size;"`
- `run_demo_trace.sh:137` — `PGOPTIONS='-c wait_event_capture=trace'`
- `run_demo_trace.sh:441` — `PGOPTIONS='-c wait_event_capture=trace'`
- `run_demo_v2.sh:108` — `run_sql "SHOW wait_event_capture; ...`
- `run_demo_v2.sh:341` — `PGOPTIONS='-c wait_event_capture=trace'`

All nine lines need `wait_event_capture` → `pg_wait_event_tracing.capture`
and `wait_event_trace_ring_size` → `pg_wait_event_tracing.trace_ring_size`
before these scripts will work against this branch; none touched here.

## Docs build/validation result

`xmllint` and `xsltproc` are **not installed** on this host (`which`/
`command -v` both fail for both, and neither `libxml2-utils` nor
`libxslt1-dev`/tools package is present — only `libxslt1.1`, the runtime
library, is). I did not install anything, per the hard rule. I also
confirmed by reading `doc/src/sgml/meson.build` directly
(lines 85–86: `if not xmllint_bin.found() subdir_done() endif`) that
without `xmllint` the entire docs subdirectory produces **zero** build
targets — there is no partial "validate the SGML" target to fall back to,
and a `meson setup` would not even reach the point of registering one. I
therefore did not run `meson setup`/`ninja` at all for docs, since it
would provide no additional signal beyond what reading the build file
already shows, and would cost setup time/disk for nothing.

As a best-effort substitute (explicitly **not** equivalent to real
DocBook DTD validation via `xmllint --valid`), I:

- Wrote a small Python check that strips comments, neutralizes named
  entities (keeping the five XML builtins), wraps the file in a
  synthetic root element, and parses it with `xml.etree.ElementTree` —
  confirms tag balance/nesting only. `pgwaiteventtracing.sgml`,
  `xfunc.sgml`, and `contrib.sgml` all pass this check after the edits.
- Extracted every `id="..."` in the new file (29 ids) and confirmed none
  collides with an existing id anywhere else in `doc/src/sgml/*.sgml`.
- Extracted every `linkend="..."` used in the new file (internal and the
  four external: `guc-shared-preload-libraries`, `predefined-roles`,
  `guc-compute-query-id`, `xfunc-addin-wait-events`) and confirmed each
  resolves to an existing `id=` somewhere in the tree, and that every
  internal linkend resolves to an id defined in the same file.

This gives reasonable confidence against gross errors (unclosed tags,
duplicate ids, dangling xrefs) but does **not** validate against the
actual DocBook DTD (element content models, attribute lists, table
column-count consistency, etc.) the way `ninja docs` would. That real
validation still needs to run wherever `xmllint`/`xsltproc` are
available (the fork's CI, or the reviewer's own host).

## Disk and build hygiene

No `meson setup`/`ninja` build directory was created for this WP (nothing
to build: no server was started, and the only allowed build target —
docs — has no available toolchain, as established above). No files were
left in the working directory beyond the repository's own tracked
changes; the worktree at `<workspace>/wt-v8-wp5`
remains as created, on branch `wet-v8-wp5`, three commits ahead of
`wet-v8-wp3`. The main checkout stayed on `wet-series` throughout, and
nothing was pushed.

## Where the code and the plan disagreed (docs follow the code)

1. **Histogram-buckets view and capacity function have no explicit
   grant.** `pg_wait_event_timing_histogram_buckets` (a view) and
   `pg_wait_event_tracing_capacity()` (a function) carry no
   `REVOKE`/`GRANT` in the extension script at all, unlike every other
   view/function in the module. By ordinary PostgreSQL default
   privileges this means: the capacity function keeps its default
   `PUBLIC` execute privilege (harmless — it exposes only compiled-in
   constants), but the histogram-buckets view, having no privileges
   explicitly granted to `PUBLIC`, is selectable only by its owner and by
   superusers — even though it is pure static metadata (bucket
   boundaries) with no reason to be locked down, and every other public
   view in the module explicitly grants `SELECT` to `pg_read_all_stats`.
   This looks like an oversight in the extension script rather than an
   intentional restriction. Documented exactly as the script currently
   behaves, per the hard rule that the docs follow the code; flagging it
   here since a one-line `GRANT SELECT ON pg_wait_event_timing_histogram_buckets
   TO PUBLIC;` (it is static, read-only content, safe for anyone to see)
   would be a natural, low-risk fix before this module ships.
2. **D2's "the example."** Already covered above under "D2 — where the
   code example lives": the brief's phrasing names the wrong sect2 for
   the actual code sample; I used the one sect2 that has a real example
   to switch, and documented the reasoning in this report.
3. Everything else in the code matched the plan/overrides closely enough
   that no other disagreement worth flagging came up during this WP.
