package frankenphp_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dunglas/frankenphp"
)

// the benchmarks drive task-bench.php, which loops send/read inside PHP: one
// request per b.N tasks, so the request overhead vanishes in the average

type taskBenchResult struct {
	N         int `json:"n"`
	PerTaskNs int `json:"per_task_ns"`
	SendNs    int `json:"send_ns"`
	ReadNs    int `json:"read_ns"`
	PickupNs  int `json:"pickup_ns"`
	Worker    *struct {
		Tasks        int  `json:"tasks"`
		Waits        bool `json:"waits"`
		Wakeups      int  `json:"wakeups"`
		EmptyWakeups int  `json:"empty_wakeups"`
		ReceiveNs    int  `json:"receive_ns"`
		UpdateNs     int  `json:"update_ns"`
		CompleteNs   int  `json:"complete_ns"`
	} `json:"worker"`
}

func taskBenchRequest(b *testing.B, server *frankenphp.Server, query string) taskBenchResult {
	b.Helper()
	w := httptest.NewRecorder()
	if err := server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://example.com/task-bench.php?"+query, nil)); err != nil {
		b.Fatal(err)
	}
	body, _ := io.ReadAll(w.Result().Body)
	var r taskBenchResult
	if err := json.Unmarshal(body, &r); err != nil {
		b.Fatalf("%v: %s", err, body)
	}

	return r
}

// cpuTime is the CPU time the process used so far, all threads included
func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// receive picks how the worker waits for its tasks: "wait" in receive() where
// the build has it, anything else on its stream
func initTaskBench(b *testing.B, num, numThreads int, receive string) *frankenphp.Server {
	b.Helper()
	server, err := frankenphp.NewServer(testDataDir)
	if err != nil {
		b.Fatal(err)
	}
	if err := frankenphp.Init(
		frankenphp.WithServer(server),
		frankenphp.WithWorkers("echo", "testdata/bgworker/task-bench-worker.php", num,
			frankenphp.WithWorkerBackground(),
			frankenphp.WithWorkerServerScope(server),
			frankenphp.WithWorkerTasks("echo"),
			frankenphp.WithWorkerEnv(map[string]string{"BG_RECEIVE": receive}),
		),
		frankenphp.WithNumThreads(numThreads),
	); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(frankenphp.Shutdown)

	return server
}

func benchmarkTask(b *testing.B, receive string) {
	for _, tc := range []struct {
		name  string
		num   int
		query string
	}{
		{"num=1", 1, ""},
		{"num=4", 4, ""},
		{"size=64KB", 1, "&size=65536"},
		{"elements=1000", 1, "&elements=1000"},
		{"updates=16", 1, "&updates=16"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			server := initTaskBench(b, tc.num, 1, receive)
			// warm up the worker and opcache
			taskBenchRequest(b, server, "n=100"+tc.query)
			b.ResetTimer()
			cpu := cpuTime()
			r := taskBenchRequest(b, server, "n="+strconv.Itoa(b.N)+tc.query)
			cpu = cpuTime() - cpu
			b.StopTimer()
			b.ReportMetric(float64(r.SendNs), "send-ns/task")
			b.ReportMetric(float64(r.ReadNs), "read-ns/task")
			b.ReportMetric(float64(r.PickupNs), "pickup-ns/task")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/task")
			if r.Worker != nil {
				b.ReportMetric(float64(r.Worker.UpdateNs), "update-ns/task")
				b.ReportMetric(float64(r.Worker.CompleteNs), "complete-ns/task")
			}
		})
	}
}

// BenchmarkTaskWorkWait gives the worker CPU work to do per task: the sender
// waits that long for its answer, whether it sleeps or keeps a core busy
// meanwhile shows in cpu-ns/task. mix alternates tasks with and without work
func BenchmarkTaskWorkWait(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"work=100us", "&work_us=100"},
		{"work=1ms", "&work_us=1000"},
		{"mix=0+200us", "&work_us=200&mix=1"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			server := initTaskBench(b, 1, 1, "wait")
			taskBenchRequest(b, server, "n=100"+tc.query)
			b.ResetTimer()
			cpu := cpuTime()
			r := taskBenchRequest(b, server, "n="+strconv.Itoa(b.N)+tc.query)
			cpu = cpuTime() - cpu
			b.StopTimer()
			b.ReportMetric(float64(r.SendNs), "send-ns/task")
			b.ReportMetric(float64(r.ReadNs), "read-ns/task")
			b.ReportMetric(float64(r.PickupNs), "pickup-ns/task")
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/task")
		})
	}
}

// BenchmarkTask: the worker waits on its stream between two tasks
func BenchmarkTask(b *testing.B) { benchmarkTask(b, "stream") }

// BenchmarkTaskWait: the worker waits in receive(), where the build has it
func BenchmarkTaskWait(b *testing.B) { benchmarkTask(b, "wait") }

// ns/op is per batch of k tasks, sent to a pool of k threads before the first
// one is read
func benchmarkTaskFanOut(b *testing.B, receive string) {
	for _, k := range []int{4} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			server := initTaskBench(b, k, 1, receive)
			query := "&fanout=" + strconv.Itoa(k)
			taskBenchRequest(b, server, "n=100"+query)
			b.ResetTimer()
			r := taskBenchRequest(b, server, "n="+strconv.Itoa(b.N)+query)
			b.StopTimer()
			b.ReportMetric(float64(r.SendNs), "send-ns/task")
			b.ReportMetric(float64(r.ReadNs), "read-ns/task")
			b.ReportMetric(float64(r.PickupNs), "pickup-ns/task")
		})
	}
}

// BenchmarkTaskFanOut sends a batch of tasks to a pool, then reads them all
func BenchmarkTaskFanOut(b *testing.B) { benchmarkTaskFanOut(b, "stream") }

// BenchmarkTaskFanOutWait is the same with workers waiting in receive()
func BenchmarkTaskFanOutWait(b *testing.B) { benchmarkTaskFanOut(b, "wait") }

// ns/op is per request of 100 tasks, tasks/s is the throughput, the other
// metrics are averages over all the tasks
func benchmarkTaskConcurrent(b *testing.B, receive string) {
	for _, tc := range []struct{ senders, num int }{{1, 1}, {4, 4}, {8, 8}, {16, 8}} {
		b.Run(fmt.Sprintf("senders=%d/num=%d", tc.senders, tc.num), func(b *testing.B) {
			server := initTaskBench(b, tc.num, tc.senders, receive)
			taskBenchRequest(b, server, "n=100")
			b.ResetTimer()
			var (
				wg                          sync.WaitGroup
				mu                          sync.Mutex
				sendNs, readNs, pickup, cnt float64
			)
			requests := make(chan struct{}, b.N)
			for range b.N {
				requests <- struct{}{}
			}
			close(requests)
			for range tc.senders {
				wg.Go(func() {
					for range requests {
						r := taskBenchRequest(b, server, "n=100")
						mu.Lock()
						sendNs += float64(r.SendNs)
						readNs += float64(r.ReadNs)
						pickup += float64(r.PickupNs)
						cnt++
						mu.Unlock()
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "tasks/s")
			b.ReportMetric(sendNs/cnt, "send-ns/task")
			b.ReportMetric(readNs/cnt, "read-ns/task")
			b.ReportMetric(pickup/cnt, "pickup-ns/task")
		})
	}
}

// BenchmarkTaskConcurrent hammers a pool from several request threads at once
func BenchmarkTaskConcurrent(b *testing.B) { benchmarkTaskConcurrent(b, "stream") }

// BenchmarkTaskConcurrentWait is the same with workers waiting in receive()
func BenchmarkTaskConcurrentWait(b *testing.B) { benchmarkTaskConcurrent(b, "wait") }

// BenchmarkTaskBaseline is the cost of a request to a trivial script, for
// scale
func BenchmarkTaskBaseline(b *testing.B) {
	server := initTaskBench(b, 1, 1, "stream")
	b.ResetTimer()
	for range b.N {
		w := httptest.NewRecorder()
		if err := server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://example.com/index.php", nil)); err != nil {
			b.Fatal(err)
		}
	}
}
