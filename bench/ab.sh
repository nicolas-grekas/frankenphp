#!/bin/bash
# A/B of the task API between checkouts, alternating them over several rounds
# so they all see the same machine state. Args: rounds, then label=dir pairs;
# the first one is the reference of the deltas.
set -e
ROUNDS=$1; shift
HARNESS="workertask_bench_test.go testdata/task-bench.php testdata/bgworker/task-bench-worker.php"
for pair in "$@"; do
  label=${pair%%=*}; dir=${pair#*=}
  for f in $HARNESS; do mkdir -p "$dir/$(dirname "$f")"; cp "$f" "$dir/$f"; done
  (cd "$dir" && go test -c -o "/tmp/$label.test" .)
done
BENCH='^BenchmarkTask(Wait)?$/^(num=1|updates=16)$|^BenchmarkTaskFanOut(Wait)?$|^BenchmarkTaskConcurrent(Wait)?$/^senders=(1|4|8|16)$/^num=(1|4|8)$'
: > /tmp/results.txt
for r in $(seq 1 "$ROUNDS"); do
  for pair in "$@"; do
    label=${pair%%=*}; dir=${pair#*=}
    (cd "$dir" && "/tmp/$label.test" -test.run xxx -test.bench "$BENCH" -test.benchtime=2s -test.count=1 2>&1 \
      | grep -E '^Benchmark' | sed "s/^/$label /" | tee -a /tmp/results.txt)
  done
done
echo
echo "## Averages over $ROUNDS rounds" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
python3 - "$@" <<'PY' | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
import re, sys, collections
sides = [x.split("=")[0] for x in sys.argv[1:]]
acc = collections.defaultdict(list)
extra = collections.defaultdict(lambda: collections.defaultdict(list))
for line in open("/tmp/results.txt"):
    parts = line.split()
    side, name = parts[0], re.sub(r"-\d+$", "", parts[1])
    m = re.search(r"\s(\d+) ns/op", line)
    t = re.search(r"([\d.]+) tasks/s", line)
    acc[(name, side)].append(float(t.group(1)) if t else int(m.group(1)) / 1000)
    for v, k in re.findall(r"([\d.]+) ((?:send|read|pickup)-ns/task)", line):
        extra[(name, side)][k].append(float(v) / 1000)
print("| benchmark | " + " | ".join(sides) + " |")
print("|---|" + "---|" * len(sides))
for name in sorted({k[0] for k in acc}):
    row, base = [], None
    for s in sides:
        v = acc.get((name, s))
        if not v:
            row.append("-")
            continue
        mean = sum(v) / len(v)
        throughput = "Concurrent" in name
        cell = ("%.0f tasks/s" if throughput else "%.1f us") % mean
        if base is None:
            base = mean
        else:
            cell += " (%+.1f%%)" % ((mean / base - 1) * 100)
        e = extra.get((name, s), {})
        if e and not throughput:
            cell += " [send %.1f, read %.1f, pickup %.1f]" % tuple(sum(e[k]) / len(e[k]) for k in ("send-ns/task", "read-ns/task", "pickup-ns/task"))
        row.append(cell)
    print("| %s | %s |" % (name.replace("Benchmark", ""), " | ".join(row)))
PY
if command -v strace >/dev/null; then
  for bench in '^BenchmarkTask$/^num=1$' '^BenchmarkTaskWait$/^num=1$'; do
    echo; echo "## Syscalls per task, $bench (5203 tasks with the warm-ups)" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
    for pair in "$@"; do
      label=${pair%%=*}; dir=${pair#*=}
      (cd "$dir" && strace -f -c -o "/tmp/strace-$label.txt" "/tmp/$label.test" -test.run xxx -test.bench "$bench" -test.benchtime=5000x -test.count=1 >/dev/null 2>&1) || true
      echo "- $label: $(awk -v n=5203 '$4 ~ /^[0-9]+$/ && $NF !~ /total/ && NF>=5 && $4/n >= 0.3 { printf "%s %.2f, ", $NF, $4/n } /total/ { printf "total %.2f", $4/n }' "/tmp/strace-$label.txt")" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
    done
  done
fi
