package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

var etcdURL string
var tmpDirRoot = os.Getenv("XDG_RUNTIME_DIR") // when unset "" falls back to default

func TestDB(t *testing.T) {
	etcdDeleteAll(t)

	ctx := context.Background()

	k1 := T2{F1: "f1", F2: true}

	t.Run("readwrite", func(t *testing.T) {
		opts := optsTFunc
		opts.Logf = t.Logf
		db, err := New(ctx, etcdURL, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		tx := db.Tx(context.Background())
		tx.Put("/db/t2/k1", k1)
		tx.Put("/db/t2/k2", T2{F1: "f2", F2: true})
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		//defer db.Close()
		if gotk1, err := db.Tx(context.Background()).Get("/db/t2/k1"); err != nil {
			t.Fatal(err)
		} else if gotk1 != k1 {
			t.Errorf("/db/t2/k1=%v, want %v", gotk1, k1)
		}
	})

	t.Run("readwrite-newdb", func(t *testing.T) {
		opts := optsTFunc
		opts.Logf = t.Logf
		db, err := New(ctx, etcdURL, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if gotk1, err := db.Tx(context.Background()).Get("/db/t2/k1"); err != nil {
			t.Fatal(err)
		} else if gotk1 != k1 {
			t.Errorf("/db/t2/k1=%v, want %v", gotk1, k1)
		}
		if gotk1, err := db.ReadTx().Get("/db/t2/k1"); err != nil {
			t.Fatal(err)
		} else if gotk1 != k1 {
			t.Errorf("/db/t2/k1=%v, want %v", gotk1, k1)
		}
	})
}

func TestStaleTx(t *testing.T) {
	etcdDeleteAll(t)

	watchCh := make(chan []KV, 8)
	checkWatch := func(want []KV) {
		t.Helper()
		select {
		case got := <-watchCh:
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Watch=%v, want %v", got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no watch update")
		}
	}
	checkNoWatch := func() {
		t.Helper()
		select {
		case unexpected := <-watchCh:
			t.Errorf("unexpected watch update: %v", unexpected)
		default:
		}
	}

	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	v1 := T2{F1: "f1", F2: true}

	// set key
	func() {
		tx := db.Tx(ctx)
		if err := tx.Put("/db/t2/k1", v1); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		checkWatch([]KV{{"/db/t2/k1", v1}})
		checkNoWatch()
	}()

	// no-op replace key
	func() {
		tx := db.Tx(ctx)
		if err := tx.Put("/db/t2/k1", v1); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		checkWatch([]KV{{"/db/t2/k1", v1}})
		checkNoWatch()
	}()

	// update key
	func() {
		tx := db.Tx(ctx)
		v2 := T2{F1: "f2", F2: true}
		if err := tx.Put("/db/t2/k1", v2); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		checkWatch([]KV{{"/db/t2/k1", v2}})
		checkNoWatch()
	}()

	// stale write
	func() {
		tx := db.Tx(ctx)
		tx.Get("/db/t2/k1")
		if err := tx.Put("/db/t2/k1", T2{F1: "f4", F2: true}); err != nil {
			t.Fatal(err)
		}

		v3 := T2{F1: "f3", F2: true}
		tx2 := db.Tx(ctx)
		if err := tx2.Put("/db/t2/k1", v3); err != nil {
			t.Fatal(err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatal(err)
		}
		checkWatch([]KV{{"/db/t2/k1", v3}})
		checkNoWatch()

		if err := tx.Commit(); err == nil || !errors.Is(err, ErrTxStale) {
			t.Errorf("err=%v, want ErrTxStale", err)
		}
		checkNoWatch()
	}()

	// stale read
	func() {
		tx := db.Tx(ctx)
		if _, err := tx.Get("/db/t2/k1"); err != nil {
			t.Fatal(err)
		}

		v3 := T2{F1: "f3", F2: true}
		tx2 := db.Tx(ctx)
		if err := tx2.Put("/db/t2/k1", v3); err != nil {
			t.Fatal(err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatal(err)
		}
		checkWatch([]KV{{"/db/t2/k1", v3}})

		if _, err := tx.Get("/db/t2/k1"); err == nil || !errors.Is(err, ErrTxStale) {
			t.Errorf("err=%v, want ErrTxStale", err)
		}
	}()
}

func TestVariableKeys(t *testing.T) {
	etcdDeleteAll(t)

	watchCh := make(chan []KV, 8)
	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		for j := 1; j < 5; j++ {
			var want []KV
			for k := 0; k < j; k++ {
				key := fmt.Sprintf("/db/t2/k%d", k)
				want = append(want, KV{key, T2{F1: key, F2: true}})
			}
			tx := db.Tx(ctx)
			for _, kv := range want {
				if err := tx.Put(kv.Key, kv.Value); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			select {
			case got := <-watchCh:
				if !reflect.DeepEqual(got, want) {
					t.Errorf("i=%d, j=%d, Watch=%v, want %v", i, j, got, want)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("i=%d, j=%d, no watch update", i, j)
			}
			select {
			case unexpected := <-watchCh:
				t.Errorf("i=%d, j=%d, unexpected watch update: %v", i, j, unexpected)
			default:
			}
		}
	}
}

func TestExternalValue(t *testing.T) {
	etcdDeleteAll(t)

	watchCh := make(chan []KV, 8)
	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "/db/t2/k1"
	val := T2{F1: "cmdline"}

	valBytes, err := json.Marshal(val)
	if err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command("etcdctl", "--endpoints="+etcdURL, "put", key, string(valBytes)).CombinedOutput()
	if err != nil {
		t.Fatalf("etcdctl put failed: %v, stderr:\n%s", err, out)
	}

	want := []KV{{key, val}}

	select {
	case got := <-watchCh:
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Watch=%v, want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no watch update")
	}
}

func TestLoadPageLimit(t *testing.T) {
	etcdDeleteAll(t)
	ctx := context.Background()
	db, err := New(ctx, etcdURL, Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Tx(ctx)
	tx.Put("/a1", []byte("first"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx = db.Tx(ctx)
	for i := 0; i < loadPageLimit; i++ {
		tx.Put(fmt.Sprintf("/b%d", i), []byte("second"))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx = db.Tx(ctx)
	for i := 0; i < loadPageLimit; i++ {
		tx.Put(fmt.Sprintf("/c%d", i), []byte("second"))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	numKVs := 0
	opts := Options{
		Logf:      t.Logf,
		WatchFunc: func(kvs []KV) { numKVs += len(kvs) },
	}
	db, err = New(ctx, etcdURL, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if want := 201; numKVs != want {
		t.Errorf("numKVs=%d, want %d", numKVs, want)
	}
}

func TestGetRange(t *testing.T) {
	etcdDeleteAll(t)
	ctx := context.Background()
	db, err := New(ctx, etcdURL, Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Tx(ctx)
	tx.Put("/a/1", []byte("1"))
	tx.Put("/a/2", []byte("2"))
	tx.Put("/a/3", []byte("3"))
	tx.Put("/b/1", []byte("b1"))
	tx.Put("/b/2", []byte("b2"))
	tx.Put("/b/3", []byte("b3"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	func() {
		var kvs []KV
		if err := db.ReadTx().GetRange("/a/", func(k []KV) { kvs = append(kvs, k...) }); err != nil {
			t.Fatal(err)
		}
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		want := []KV{{"/a/1", []byte("1")}, {"/a/2", []byte("2")}, {"/a/3", []byte("3")}}
		if !reflect.DeepEqual(want, kvs) {
			t.Errorf(`GetRange("/a/")=%v, want %v`, kvs, want)
		}
	}()

	func() {
		var kvs []KV
		if err := db.ReadTx().GetRange("/b/", func(k []KV) { kvs = append(kvs, k...) }); err != nil {
			t.Fatal(err)
		}
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		want := []KV{{"/b/1", []byte("b1")}, {"/b/2", []byte("b2")}, {"/b/3", []byte("b3")}}
		if !reflect.DeepEqual(want, kvs) {
			t.Errorf(`GetRange("/b/")=%v, want %v`, kvs, want)
		}
	}()
}

type T1 struct {
	F1 string `json:"f1"`
	I2 int    `json:"i1"`
}

type T2 struct {
	F1 string `json:"f1"`
	F2 bool   `json:"f2"`
}

var optsTFunc = Options{
	KeyPrefix:  "/db/",
	EncodeFunc: encodeTFunc,
	DecodeFunc: decodeTFunc,
}

func encodeTFunc(key string, value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch {
	case strings.HasPrefix(key, "/db/t2"):
		return json.Marshal(value)
	default:
		b, isBytes := value.([]byte)
		if isBytes {
			return b, nil
		}
		return nil, fmt.Errorf("encodeTFunc: unknown value type %T", value)
	}
}

func decodeTFunc(key string, data []byte) (interface{}, error) {
	switch {
	case strings.HasPrefix(key, "/db/t2"):
		var t2 T2
		if err := json.Unmarshal(data, &t2); err != nil {
			return nil, err
		}
		return t2, nil
	default:
		return data, nil
	}
}

func BenchmarkPutOver(b *testing.B) {
	etcdDeleteAll(b)

	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Tx(ctx)
		tx.Put("/db/t2/k1", T1{F1: "a benchmark value", I2: i})
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut(b *testing.B) {
	etcdDeleteAll(b)

	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Tx(ctx)
		tx.Put(fmt.Sprintf("/db/t2/k%d", i), T1{F1: "a benchmark value", I2: i})
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutX2(b *testing.B) { benchmarkPutX(b, 2) }
func BenchmarkPutX4(b *testing.B) { benchmarkPutX(b, 4) }
func BenchmarkPutX8(b *testing.B) { benchmarkPutX(b, 8) }

func benchmarkPutX(b *testing.B, x int) {
	etcdDeleteAll(b)

	ctx := context.Background()
	opts := optsTFunc
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	limit := make(chan struct{}, x)
	for i := 0; i < cap(limit); i++ {
		limit <- struct{}{}
	}
	errch := make(chan error)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case <-limit:
		case err := <-errch:
			b.Fatal(err)
		}
		go func(i int) {
			defer func() {
				limit <- struct{}{}
			}()
			tx := db.Tx(ctx)
			tx.Put(fmt.Sprintf("/db/t2/k%d", i), T1{F1: "a benchmark value", I2: i})
			if err := tx.Commit(); err != nil {
				errch <- err
			}
		}(i)
	}
	for i := 0; i < cap(limit); i++ {
		select {
		case err := <-errch:
			b.Fatal(err)
		case <-limit:
		}
	}
}

func TestMain(m *testing.M) {
	if out, err := exec.Command("which", "etcd").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot find binary 'etcd' on PATH, skipping test: %s", out)
		os.Exit(0)
	}

	url, cleanup, err := runEtcd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start etcd: %v", err)
		os.Exit(1)
	}
	etcdURL = url
	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

func etcdDeleteAll(t testing.TB) {
	t.Helper()
	out, err := exec.Command("etcdctl", "--endpoints="+etcdURL, "del", `""`, "--from-key=true").CombinedOutput()
	if err != nil {
		t.Logf("delete all failed:\n%s", out)
		t.Fatal(err)
	}
}

func runEtcd() (clientURL string, cleanup func(), err error) {
	tempDir, err := ioutil.TempDir(tmpDirRoot, "etcd-test-")
	if err != nil {
		return "", nil, err
	}

	peerURL := "http://localhost:22380"
	clientURL = "http://localhost:22379"
	dataDir := filepath.Join(tempDir, "default.etcd")
	cmd := exec.Command(
		"etcd",
		"-data-dir", dataDir,
		"-listen-peer-urls", peerURL,
		"-initial-advertise-peer-urls", peerURL,
		"-initial-cluster", "default="+peerURL,
		"-listen-client-urls", clientURL,
		"-advertise-client-urls", clientURL,
	)
	if runtime.GOARCH == "arm64" {
		cmd.Env = []string{"ETCD_UNSUPPORTED_ARCH=arm64"}
	}
	wait := &waitForStartWriter{started: make(chan struct{})}
	cmd.Stdout = wait
	cmd.Stderr = wait
	if err := cmd.Start(); err != nil {
		os.RemoveAll(tempDir)
		return "", nil, err
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-wait.started:
	case <-timer.C:
		cmd.Process.Kill()
		return "", nil, fmt.Errorf("timeout waiting for etcd to start, stderr:\n\n %s", wait.buf.Bytes())
	}
	return clientURL, func() {
		cmd.Process.Kill()
		os.RemoveAll(tempDir)
	}, nil
}

type waitForStartWriter struct {
	started chan struct{}
	buf     bytes.Buffer
}

func (w *waitForStartWriter) Write(b []byte) (int, error) {
	select {
	case <-w.started:
		return len(b), nil
	default:
	}

	w.buf.Write(b)
	if bytes.Contains(w.buf.Bytes(), []byte("ready to serve client requests")) {
		close(w.started)
	}
	return len(b), nil
}
