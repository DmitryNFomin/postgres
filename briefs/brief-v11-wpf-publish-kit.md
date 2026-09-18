# WPF: control commit, real packaging, publish kit and notes on the fork

Git and packaging work. No PostgreSQL server is started. Pushes to the
remote `fork` (DmitryNFomin/postgres) are authorized for exactly the
branches named here; use the SSH URL `git@github.com:DmitryNFomin/postgres.git`
if HTTPS has no credential (that is how WPE pushed).

Repo: `/Users/dmitryfomin/work/git/postgres` (main checkout on
REL_17_STABLE; never check it out or touch its tree). Existing
worktrees under `/Users/dmitryfomin/work/git/postgres_patch/` must not be
touched. Never bare `git stash`.

## 1. Control commit and branch

- New worktree: `git -C /Users/dmitryfomin/work/git/postgres worktree add -b bench-v11-control /Users/dmitryfomin/work/git/postgres_patch/bench-v11-control wet-v11`
- Apply `/Users/dmitryfomin/work/git/postgres_patch/v11/patches-control/0001-*.patch`
  with `git am` (it applies on top of the series; report any fuzz). Then
  `git commit --amend` so the message reads:
  subject `control: compile the timed wait-event hook sites out (benchmark layout control)`,
  body: two or three sentences saying this is the layout control for the
  hook-null contrast, used only by the benchmark kit, and is NOT part of
  the submitted series; author/committer `Dmitry Fomin <fomin.list@gmail.com>`;
  no trailers at all.
- `git diff wet-v11 bench-v11-control --stat` must be exactly
  `src/include/utils/wait_event.h`.
- Push: `git push fork bench-v11-control:bench-v11-control`. Record the
  full SHA.

## 2. Fill the hashes and package

In `/Users/dmitryfomin/work/git/postgres_patch/v11/bench-kit/sources.conf`:
```
MASTER_SHA=311df1dc0392f06973cf98eac51d63cb007267ce
V11_SHA=96c24a28006fb6ff66264998c698f5230d32ad26
CONTROL_SHA=<full sha from step 1>
```
Verify with `git -C /Users/dmitryfomin/work/git/postgres cat-file -t <sha>`
for all three, and that `git ls-remote fork` shows wet-v11 at V11_SHA and
bench-v11-control at CONTROL_SHA.

Then run the real packaging:
```
cd /Users/dmitryfomin/work/git/postgres_patch/v11/bench-kit
POSTGRES_REPO_PATH=/Users/dmitryfomin/work/git/postgres ./make-baremetal-package.sh /Users/dmitryfomin/work/git/postgres_patch/v11/dist
```
Confirm: the archive and `.sha256` exist; `tar tzf` lists the kit
scripts, workloads, `crossover/`, the fixture, the three source
snapshots, both patch sets and the manifest; `sha256sum -c` (or `shasum
-a 256 -c`) passes; report the archive size. Confirm the crossover stage
is invoked by `run-benchmark.sh` from inside this package (grep it) and
that the runbook describes it; if the crossover has its own separate
package step that the executor must run, say so explicitly in the README
of step 3.

## 3. Publish the notes branch

Create an ORPHAN branch `v11-notes` in a fresh worktree
(`git -C /Users/dmitryfomin/work/git/postgres worktree add --detach /Users/dmitryfomin/work/git/postgres_patch/v11-notes-wt origin/master`
then `git checkout --orphan v11-notes` and `git rm -rfq .` inside it), and
copy in from `/Users/dmitryfomin/work/git/postgres_patch/v11/`:
`bench-kit/` (without `__pycache__` and `.DS_Store`), `patches-v11/`,
`patches-control/`, `reports/`, `briefs/`, `codegen/`, and a copy of
`bench-kit/BAREMETAL-RUNBOOK-v11.md` at the top level. Do NOT include
`dist/` (too large for git) or `patches-wpa/`/`patches-wpb/` (folded).

Write a top-level `README.md` covering: what v11 is (five patches on
master 311df1dc039, tip 96c24a28006, branch `wet-v11`; control branch
`bench-v11-control`); what changed versus v8 in three bullets (cold
out-of-line hook path with the hint; lazy per-process hook install and
non-chaining variant; single recording gate and local in-flight state);
how to produce the benchmark package on a machine with GitHub access:
```
git clone git@github.com:DmitryNFomin/postgres.git pg && cd pg
git fetch origin wet-v11 bench-v11-control
git worktree add ../v11-notes origin/v11-notes   # or checkout
cd ../v11-notes/bench-kit
POSTGRES_REPO_PATH=/path/to/pg ./make-baremetal-package.sh ../dist
```
then copy `dist/wet-v11-baremetal-r1.tar.gz` and `.sha256` to the
bare-metal host and follow `BAREMETAL-RUNBOOK-v11.md`. State the
expected duration (8 to 10 hours) and the two result files to send back.
Include the package SHA-256 you produced in step 2 so the executor can
confirm an identical build. Point to `reports/wpd-report.md` for the
codegen evidence and `reports/wpe-report.md` for the series assembly.

Commit everything as one commit, author/committer
`Dmitry Fomin <fomin.list@gmail.com>`, subject `v11 notes: benchmark kit, runbook, patches, review reports`,
no trailers. Push: `git push fork v11-notes:v11-notes`.

## 4. Verify and report

`git ls-remote --heads fork | grep -E 'wet-v11|bench-v11-control|v11-notes'`
must show the three branches at the expected SHAs. Query the CI status of
`ci/wet-v11` run 35342207165 via the GitHub API and include the per-job
conclusions.

Report: `/Users/dmitryfomin/work/git/postgres_patch/v11/reports/wpf-report.md`
with SHAs, push output, package path/size/SHA-256, tar listing summary,
CI job table, anything undone. Final chat message at most 8 lines.
