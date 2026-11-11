package frankenphp_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

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
			r := taskBenchRequest(b, server, "n="+strconv.Itoa(b.N)+tc.query)
			b.StopTimer()
			b.ReportMetric(float64(r.SendNs), "send-ns/task")
			b.ReportMetric(float64(r.ReadNs), "read-ns/task")
			b.ReportMetric(float64(r.PickupNs), "pickup-ns/task")
			if r.Worker != nil {
				b.ReportMetric(float64(r.Worker.UpdateNs), "update-ns/task")
				b.ReportMetric(float64(r.Worker.CompleteNs), "complete-ns/task")
			}
		})
	}
}

// BenchmarkTask: the worker waits on its stream between two tasks
func BenchmarkTask(b *testing.B) { benchmarkTask(b, "stream") }

// BenchmarkTaskWait: the worker waits in receive(), where the build has it
func BenchmarkTaskWait(b *testing.B) { benchmarkTask(b, "wait") }

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
