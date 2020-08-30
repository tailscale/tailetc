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

var etcdURLVal string

func etcdURL(tb testing.TB) string {
	if etcdURLVal == "" {
		tb.Skip("skipping test, no etcd installed in PATH")
	}
	return etcdURLVal
}

// person is a database value test object used with the keys "/db/person/".
type person struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	LikesIceCream bool   `json:"likes_ice_cream"`
}

var personOptions = Options{
	KeyPrefix: "/db/",
	EncodeFunc: func(key string, value interface{}) ([]byte, error) {
		if value == nil {
			return nil, nil
		}
		switch {
		case strings.HasPrefix(key, "/db/person"):
			return json.Marshal(value)
		default:
			b, isBytes := value.([]byte)
			if isBytes {
				return b, nil
			}
			return nil, fmt.Errorf("encodePerson: unknown value type %T", value)
		}
	},
	DecodeFunc: func(key string, data []byte) (interface{}, error) {
		switch {
		case strings.HasPrefix(key, "/db/person"):
			var p person
			if err := json.Unmarshal(data, &p); err != nil {
				return nil, err
			}
			return p, nil
		default:
			return data, nil
		}
	},
	CloneFunc: func(dst interface{}, key string, value interface{}) error {
		if value == nil {
			return nil
		}
		switch {
		case strings.HasPrefix(key, "/db/person"):
			*dst.(*person) = value.(person)
		default:
			b := value.([]byte)
			*dst.(*[]byte) = append([]byte(nil), b...)
		}
		return nil
	},
}

func TestDB(t *testing.T) {
	etcdDeleteAll(t)

	ctx := context.Background()
	alice := person{ID: 42, Name: "Alice", LikesIceCream: true}

	t.Run("readwrite", func(t *testing.T) {
		opts := personOptions
		opts.Logf = t.Logf
		db, err := New(ctx, etcdURL(t), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		tx := db.Tx(context.Background())
		tx.Put("/db/person/alice", alice)

		// add bob
		var gotBob person
		if found, err := tx.Get("/db/person/bob", &gotBob); err != nil {
			t.Fatal(err)
		} else if found {
			t.Errorf("expected no bob, got: %v", gotBob)
		}
		tx.Put("/db/person/bob", person{ID: 43, Name: "Bob"})
		if found, err := tx.Get("/db/person/bob", &gotBob); err != nil {
			t.Fatal(err)
		} else if !found {
			t.Errorf("could not get pending /db/person/bob entry")
		}
		tx.Put("/db/person/bob", person{ID: 43, Name: "Bob", LikesIceCream: true})
		if found, err := tx.Get("/db/person/bob", &gotBob); err != nil {
			t.Fatal(err)
		} else if !found {
			t.Errorf("could not get updated pending /db/person/bob entry")
		} else if !gotBob.LikesIceCream {
			t.Errorf("updated pending /db/person/bob entry LikesIceCream=false, want true")
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		var gotAlice person
		if found, err := db.Tx(context.Background()).Get("/db/person/alice", &gotAlice); err != nil {
			t.Fatal(err)
		} else if !found {
			t.Errorf("/db/person/alice not found")
		} else if gotAlice != alice {
			t.Errorf("/db/person/alice=%v, want %v", gotAlice, alice)
		}
	})

	t.Run("readwrite-newdb", func(t *testing.T) {
		opts := personOptions
		opts.Logf = t.Logf
		db, err := New(ctx, etcdURL(t), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var gotAlice person
		if found, err := db.Tx(context.Background()).Get("/db/person/alice", &gotAlice); err != nil {
			t.Fatal(err)
		} else if !found {
			t.Errorf("/db/person/alice not found")
		} else if gotAlice != alice {
			t.Errorf("/db/person/alice=%v, want %v", gotAlice, alice)
		}
		if _, err := db.ReadTx().Get("/db/person/alice", &gotAlice); err != nil {
			t.Fatal(err)
		} else if gotAlice != alice {
			t.Errorf("/db/person/alice=%v, want %v", gotAlice, alice)
		}
	})

	t.Run("newline", func(t *testing.T) {
		opts := personOptions
		opts.Logf = t.Logf
		db, err := New(ctx, etcdURL(t), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		tx := db.Tx(ctx)

		newline1 := person{Name: "line1\n"}
		newline2 := person{Name: "line1\nline2"}

		const key = "/db/person/newline"
		var pendingCalls int
		var pendingErr error
		tx.PendingUpdate = func(k string, old, new interface{}) {
			if pendingErr != nil {
				return
			}
			if k != key {
				pendingErr = fmt.Errorf("PendingUpdate call %d: key=%q, want %q", pendingCalls, k, key)
			}
			v, _ := new.(person)
			if pendingCalls == 0 && v.Name != newline1.Name {
				pendingErr = fmt.Errorf("PendingUpdate call %d: Name=%q, want %q", pendingCalls, v.Name, newline1.Name)
			}
			if pendingCalls == 1 && v.Name != newline2.Name {
				pendingErr = fmt.Errorf("PendingUpdate call %d: Name=%q, want %q", pendingCalls, v.Name, newline2.Name)
			}
			pendingCalls++
		}

		tx.Put(key, newline1)
		tx.Put(key, newline2)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx = db.ReadTx()
		var got person
		if _, err := tx.Get(key, &got); err != nil {
			t.Fatal(err)
		}
		if got != newline2 {
			t.Errorf("Get(%q) = %v, want %v", key, got, newline2)
		}

		if pendingErr != nil {
			t.Error(pendingErr)
		}
	})

}

func TestStaleTx(t *testing.T) {
	etcdDeleteAll(t)
	testStaleTx(t, etcdURL(t))
}

func TestStaleTxInMemory(t *testing.T) {
	testStaleTx(t, "memory://")
}

func testStaleTx(t *testing.T, url string) {
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
	opts := personOptions
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, url, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := person{ID: 42, Name: "Alice", LikesIceCream: true}
	aliceNoIceCream := person{ID: 42, Name: "Alice", LikesIceCream: false}

	// set key
	tx := db.Tx(ctx)
	if err := tx.Put("/db/person/alice", alice); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	checkWatch([]KV{{"/db/person/alice", nil, alice}})
	checkNoWatch()

	// no-op replace key
	tx = db.Tx(ctx)
	if err := tx.Put("/db/person/alice", alice); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	checkWatch([]KV{{"/db/person/alice", alice, alice}})
	checkNoWatch()

	// update key
	tx = db.Tx(ctx)
	if err := tx.Put("/db/person/alice", aliceNoIceCream); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	checkWatch([]KV{{"/db/person/alice", alice, aliceNoIceCream}})
	checkNoWatch()

	// stale write
	tx = db.Tx(ctx)
	var gotAlice person
	tx.Get("/db/person/alice", &gotAlice)
	if err := tx.Put("/db/person/alice", person{Name: "BadAliceTx"}); err != nil {
		t.Fatal(err)
	}

	tx2 := db.Tx(ctx)
	aliceNewID := person{ID: 4242, Name: "Alice", LikesIceCream: true}
	if err := tx2.Put("/db/person/alice", aliceNewID); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	checkWatch([]KV{{"/db/person/alice", aliceNoIceCream, aliceNewID}})
	checkNoWatch()

	if err := tx.Commit(); err == nil || !errors.Is(err, ErrTxStale) {
		t.Errorf("err=%v, want ErrTxStale", err)
	}
	checkNoWatch()

	// stale read
	tx = db.Tx(ctx)
	if _, err := tx.Get("/db/person/alice", &gotAlice); err != nil {
		t.Fatal(err)
	}

	tx2 = db.Tx(ctx)
	if err := tx2.Put("/db/person/alice", aliceNoIceCream); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	checkWatch([]KV{{"/db/person/alice", aliceNewID, aliceNoIceCream}})

	if _, err := tx.Get("/db/person/alice", &gotAlice); err == nil || !errors.Is(err, ErrTxStale) {
		t.Errorf("err=%v, want ErrTxStale", err)
	}
}

func TestVariableKeys(t *testing.T) {
	etcdDeleteAll(t)
	testVariableKeys(t, etcdURL(t))
}

func TestVariableKeysInMemory(t *testing.T) {
	testVariableKeys(t, "memory://")
}

func testVariableKeys(t *testing.T, url string) {
	watchCh := make(chan []KV, 8)
	ctx := context.Background()
	opts := personOptions
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, url, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		for j := 1; j < 5; j++ {
			var want []KV
			for k := 0; k < j; k++ {
				key := fmt.Sprintf("/db/person/k%d", k)
				val := person{Name: key}
				want = append(want, KV{key, val, val})
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
				if i > 0 && !reflect.DeepEqual(got, want) {
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
	opts := personOptions
	opts.Logf = t.Logf
	opts.WatchFunc = func(kvs []KV) {
		watchCh <- kvs
	}
	db, err := New(ctx, etcdURL(t), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := "/db/person/charlie"
	val := person{Name: "Charlie"}

	valBytes, err := json.Marshal(val)
	if err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command("etcdctl", "--endpoints="+etcdURL(t), "put", key, string(valBytes)).CombinedOutput()
	if err != nil {
		t.Fatalf("etcdctl put failed: %v, stderr:\n%s", err, out)
	}

	want := []KV{{key, nil, val}}

	select {
	case got := <-watchCh:
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Watch=%v, want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no watch update")
	}
}

func TestGetRange(t *testing.T) {
	etcdDeleteAll(t)
	testGetRange(t, etcdURL(t))
}

func TestGetRangeInMemory(t *testing.T) {
	testGetRange(t, "memory://")
}

func testGetRange(t *testing.T, url string) {
	ctx := context.Background()
	db, err := New(ctx, url, Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

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

	var kvs []KV
	fn := func(k []KV) error {
		kvs = append(kvs, k...)
		return nil
	}
	if err := db.GetRange("/a/", fn, nil); err != nil {
		t.Fatal(err)
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	want := []KV{{"/a/1", nil, []byte("1")}, {"/a/2", nil, []byte("2")}, {"/a/3", nil, []byte("3")}}
	if !reflect.DeepEqual(want, kvs) {
		t.Errorf(`GetRange("/a/")=%v, want %v`, kvs, want)
	}

	kvs = nil
	if err := db.GetRange("/b/", fn, nil); err != nil {
		t.Fatal(err)
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	want = []KV{{"/b/1", nil, []byte("b1")}, {"/b/2", nil, []byte("b2")}, {"/b/3", nil, []byte("b3")}}
	if !reflect.DeepEqual(want, kvs) {
		t.Errorf(`GetRange("/b/")=%v, want %v`, kvs, want)
	}
}

func BenchmarkPutOver(b *testing.B) {
	etcdDeleteAll(b)

	ctx := context.Background()
	opts := personOptions
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL(b), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	alice := person{ID: 42, Name: "Alice MacDuff", LikesIceCream: true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Tx(ctx)
		tx.Put("/db/person/alice", alice)
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut(b *testing.B) {
	etcdDeleteAll(b)

	ctx := context.Background()
	opts := personOptions
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL(b), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	alice := person{ID: 42, Name: "Alice MacDuff", LikesIceCream: true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.Tx(ctx)
		tx.Put("/db/person/alice", alice)
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
	opts := personOptions
	opts.Logf = b.Logf
	db, err := New(ctx, etcdURL(b), opts)
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
			tx.Put(fmt.Sprintf("/db/person/k%d", i), person{ID: i, LikesIceCream: true})
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
	if _, err := exec.Command("which", "etcd").CombinedOutput(); err != nil {
		etcdURLVal = ""
		os.Exit(m.Run())
	}

	url, cleanup, err := runEtcd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start etcd: %v", err)
		os.Exit(1)
	}
	etcdURLVal = url
	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

func etcdDeleteAll(t testing.TB) {
	t.Helper()
	out, err := exec.Command("etcdctl", "--endpoints="+etcdURL(t), "del", `""`, "--from-key=true").CombinedOutput()
	if err != nil {
		t.Logf("delete all failed:\n%s", out)
		t.Fatal(err)
	}
}

func runEtcd() (clientURL string, cleanup func(), err error) {
	tempDir, err := ioutil.TempDir("", "etcd-test-")
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
