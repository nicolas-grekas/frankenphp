#!/bin/bash
# A/B of the task API between two checkouts, alternating them over several
# rounds so both see the same machine state. Args: dir A, dir B, rounds.
set -e
A=$1; B=$2; ROUNDS=${3:-4}
HARNESS="workertask_bench_test.go testdata/task-bench.php testdata/bgworker/task-bench-worker.php"
for side in "$A" "$B"; do
  for f in $HARNESS; do mkdir -p "$side/$(dirname "$f")"; cp "$f" "$side/$f"; done
  (cd "$side" && go test -c -o "/tmp/$(basename "$side").test" .)
done
BENCH='BenchmarkTask/num=1$|BenchmarkTask/updates=16$|BenchmarkTaskConcurrent/senders=(1|4|8)/num=(1|4|8)$'
: > /tmp/results.txt
for r in $(seq 1 "$ROUNDS"); do
  for side in "$A" "$B"; do
    name=$(basename "$side")
    "/tmp/$name.test" -test.run xxx -test.bench "$BENCH" -test.benchtime=2s -test.count=1 2>&1 \
      | grep -E '^Benchmark' | sed "s/^/$name /" | tee -a /tmp/results.txt
  done
done
echo
echo "## Averages over $ROUNDS rounds" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
python3 - "$A" "$B" <<'PY' | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
import re, sys, collections, os
a, b = [os.path.basename(x) for x in sys.argv[1:3]]
acc = collections.defaultdict(list)
for line in open("/tmp/results.txt"):
    parts = line.split()
    side, name = parts[0], parts[1].split("-")[0]
    m = re.search(r"\s(\d+) ns/op", line); ns = int(m.group(1)) if m else None
    t = re.search(r"([\d.]+) tasks/s", line)
    acc[(name, side)].append((ns, float(t.group(1)) if t else None))
print("| benchmark | %s | %s | delta |" % (a, b)); print("|---|---|---|---|")
for name in sorted({k[0] for k in acc}):
    va, vb = acc.get((name, a), []), acc.get((name, b), [])
    if not va or not vb: continue
    if va[0][1] is not None:
        ma, mb = sum(x[1] for x in va)/len(va), sum(x[1] for x in vb)/len(vb)
        print("| %s | %.0f tasks/s | %.0f tasks/s | %+.1f%% |" % (name, ma, mb, (mb/ma-1)*100))
    else:
        ma, mb = sum(x[0] for x in va)/len(va), sum(x[0] for x in vb)/len(vb)
        print("| %s | %.1f us | %.1f us | %+.1f%% |" % (name, ma/1000, mb/1000, (mb/ma-1)*100))
PY
if command -v strace >/dev/null; then
  echo; echo "## Syscalls per task (5000 tasks + warmup)" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  for side in "$A" "$B"; do
    name=$(basename "$side")
    strace -f -c -o /tmp/strace-$name.txt "/tmp/$name.test" -test.run xxx -test.bench 'BenchmarkTask/num=1$' -test.benchtime=5000x -test.count=1 >/dev/null 2>&1 || true
    echo "- $name: $(awk -v n=5100 '$4 ~ /^[0-9]+$/ && $NF !~ /total/ && NF>=5 && $4/n >= 0.5 { printf "%s %.2f, ", $NF, $4/n } /total/ { printf "total %.2f", $4/n }' /tmp/strace-$name.txt)" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  done
fi
