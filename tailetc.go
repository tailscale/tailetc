// Package tailetc implements an total-memory-cache etcd v3 client
// implemented by tailing (watching) an entire etcd.
//
// The client maintains a complete copy of the etcd database in memory,
// with values decoded into Go objects.
//
//
// Transaction Model
//
// It presents a simplified transaction model that assumes that multiple
// writers will not be contending on keys. When you call tx.Get or
// tx.Put, your Tx records the current etcd global revision.
// When you tx.Commit, if some newer revision of any key touched by Tx
// is in etcd then the commit will fail.
//
// Contention failures are reported as ErrTxStale from tx.Commit.
// Failures may be reported sooner as tx.Get or tx.Put errors, but the
// tx error is sticky, that is, if you ignore those errors the eventual
// error from tx.Commit will have ErrTxStale in its error chain.
//
// The Tx.Commit method waits before successfully returning until DB
// has caught up with the etcd global revision of the transaction.
// This ensures that sequential happen in the strict database sequence.
// So if you serve a REST API from a single *etcd.DB instance, then the
// API will behave as users expect it to.
//
// If you know only one thing about this etcd client, know this:
//
//	Do not have writer contention on individual keys.
//
// Everything else should follow a programmer's intuition for an
// in-memory map guarded by a RWMutex.
//
//
// Caching
//
// We take advantage of etcd's watch model to maintain a *decoded*
// value cache of the entire database in memory. This means that
// querying a value is extremely cheap. Issuing tx.Get(key) involves
// no more work than holding a mutex read lock, reading from a map,
// and cloning the value.
//
// The etcd watch is then exposed to the user of *etcd.DB via the
// WatchFunc. This lets users maintain in-memory higher-level indexes
// into the etcd database that respond to external commits.
// WatchFunc is called while the database lock is held, so a WatchFunc
// implementation cannot synchronously issue transactions.
//
//
// Object Ownership
//
// The cache in etcd.DB is very careful not to copy objects both into
// and out of the cache so that users of the etcd.DB cannot get pointers
// directly into the memory inside the cache. This means over-copying.
// Users can control precisely how much copying is done, and how, by
// providing a CloneFunc implementation.
//
// The general ownership semantics are: objects are copied as soon as
// they are passed to etcd, and any memory returned by etcd is owned
// by the caller.
package tailetc

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"tailscale.com/syncs"
)

// ErrTxStale is reported when another transaction has modified a key
// referenced by this transaction, so it can no longer be applied to
// the database.
var ErrTxStale = errors.New("tx stale")

// ErrTxClosed is reported when a method is called on a committed or
// canceled Tx.
var ErrTxClosed = errors.New("tx closed")

// dbMuLockLatency reports the most recent time it took to lock db.Mu.
var dbMuLockLatency = new(expvar.Int)

func init() {
	expvar.Publish("db_mu_lock_latency", dbMuLockLatency)
}

// DB is a read-write datastore backed by etcd.
type DB struct {
	cli      *clientv3.Client
	opts     Options
	inMemory bool // entirely in-memory

	done        <-chan struct{}
	watchCancel func()
	shutdownWG  sync.WaitGroup // shutdownWG.Add is called under mu when !closing
	embedClose  func()

	panicOnWrite int32 // testing hook: panic any time a write is requested

	// Mu is the database lock.
	//
	// Reads are guarded by read locks.
	// Transaction commits and background watch updates
	// are guarded by write lcoks.
	//
	// The mutex is exported so that higher-level wrappers that want to
	// keep indexes in sync with background updates without problematic
	// lock ordering.
	Mu sync.RWMutex

	// The following fields are guarded by Mu.

	cache          map[string]valueRev     // in-memory copy of all etcd key-values
	rev            rev                     // rev is the latest known etcd db revision
	closing        bool                    // DB.Close called
	pending        map[rev][]chan struct{} // channels to be closed when rev >= map key
	prefixWatchers map[string][]watch      // key prefix -> watch funcs
	keyWatchers    map[string][]watch      // key -> watch funcs
}

// Options are optional settings for a DB.
//
// If one of EncodeFunc, DecodeFunc, and CloneFunc are set they must all be set.
type Options struct {
	Logf  func(format string, args ...interface{})
	HTTPC *http.Client
	// KeyPrefix is a prefix on all etcd keys accessed through this client.
	// The value "" means "/", because etcd keys are file-system-like.
	KeyPrefix string
	// AuthHeader is passed as the "Authorization" header to etcd.
	AuthHeader string
	// EncodeFunc encodes values for storage.
	// If nil, the default encoder produces []byte.
	EncodeFunc func(key string, value interface{}) ([]byte, error)
	// DecodeFunc decodes values from storage.
	// If nil, the default decoder produces []byte.
	DecodeFunc func(key string, data []byte) (interface{}, error)
	// CloneFunc clones src into dst with no aliased mutable memory.
	// The definition of "aliased mutable memory" is left to the user.
	// For example, if a user is certain that no values ever passed to or
	// read from the etcd package are ever modified, use a no-op CloneFunc.
	// If nil, the default requires all values to be []byte.
	CloneFunc func(dst interface{}, key string, src interface{}) error
	// WatchFunc is called when key-value pairs change in the DB.
	//
	// When the update is a Tx from this DB, WatchFunc is called after
	// the transaction has been successfully applied by the etcd server
	// but before the Commit method returns.
	//
	// The DB.Mu write lock is held for the call, so no transactions
	// can be issued from inside WatchFunc.
	//
	// Entire etcd transactions are single calls to WatchFunc.
	//
	// The called WatchFunc owns the values passed to it.
	WatchFunc func([]KV)
	// DeleteAllOnStart deletes all keys when the client is created.
	// Used for testing.
	DeleteAllOnStart bool
}

func (opts Options) fillDefaults() (Options, error) {
	if opts.HTTPC == nil {
		opts.HTTPC = http.DefaultClient
	}
	if opts.KeyPrefix == "" {
		opts.KeyPrefix = "/"
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.EncodeFunc == nil || opts.DecodeFunc == nil || opts.CloneFunc == nil {
		if opts.EncodeFunc != nil || opts.DecodeFunc != nil || opts.CloneFunc != nil {
			return opts, fmt.Errorf("etcd: if one of EncodeFunc, DecodeFunc, CloneFunc is set, all must be set")
		}
		opts.EncodeFunc = func(key string, value interface{}) ([]byte, error) {
			if value == nil {
				return nil, nil
			}
			b, isBytes := value.([]byte)
			if !isBytes {
				return nil, fmt.Errorf("default EncodeFunc requires all values be []byte")
			}
			b2 := make([]byte, len(b))
			copy(b2, b)
			return b2, nil
		}
		opts.DecodeFunc = func(key string, data []byte) (interface{}, error) {
			return data, nil
		}
		opts.CloneFunc = func(dst interface{}, key string, value interface{}) error {
			if value == nil {
				return nil
			}
			b, isBytes := value.([]byte)
			if !isBytes {
				return fmt.Errorf("default CloneFunc requires all values be []byte")
			}
			*dst.(*[]byte) = append([]byte(nil), b...)
			return nil
		}
	}
	return opts, nil
}

// KV is a value change for an etcd key.
// Both the old value being replaced and the new value are provided, decoded.
type KV struct {
	Key      string
	OldValue interface{} // nil if there is no old value
	Value    interface{} // nil if the key has been deleted
}

// New loads the contents of an etcd prefix range and creates a *DB
// for reading and writing from the prefix range.
//
// The urls parameter is a comma-separated list of etcd HTTP endpoint,
// e.g. "http://1.1.1.1:2379,http://2.2.2.2:2379".
//
// There are two special case values of urls:
//
// If urls is "memory://", the DB does not connect to any etcd server,
// instead all operations are performed on the in-memory cache.
//
// If urls starts with the prefix "file://" then an embedded copy of etcd
// is started and creates the database in a "tailscale.etcd" directory
// under the referenced path. For example, the urls value "file:///data"
// uses an etcd database stored in "/data/tailscale.etcd".
// If only the prefix is provided, that is urls equals "file://", then
// an embedded etcd is started in the current working directory.
func New(ctx context.Context, urls string, opts Options) (db *DB, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("etcd.New: %w", err)
		}
	}()

	opts, err = opts.fillDefaults()
	if err != nil {
		return nil, err
	}

	db = &DB{
		opts:           opts,
		cache:          map[string]valueRev{},
		pending:        map[rev][]chan struct{}{},
		keyWatchers:    map[string][]watch{},
		prefixWatchers: map[string][]watch{},
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	db.done = watchCtx.Done()
	db.watchCancel = cancel

	if urls == "memory://" {
		db.inMemory = true
		return db, nil
	}
	var eps []string
	if strings.HasPrefix(urls, "file://") {
		u, err := url.Parse("http://127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		clientUrls := []url.URL{*u}

		cfg := embed.NewConfig()
		cfg.LCUrls = clientUrls
		cfg.ACUrls = clientUrls
		cfg.LPUrls = clientUrls
		cfg.APUrls = clientUrls
		cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
		cfg.Dir = filepath.Join(strings.TrimPrefix(urls, "file://"), "tailscale.etcd")
		cfg.Logger = "zap" // set to avoid data race in the default logger

		if strings.HasPrefix(cfg.Dir, os.TempDir()) {
			// Well this is a pickle.
			// The tradeoff here is startup time vs. long-running efficiency.
			// Etcd does a leader election on startup even in single-node mode,
			// and the default value of ElectionMs is 1000 meaning it takes a
			// full second to start. That really hurts tests.
			//
			// But the election timeout must be 5x the heartbeat interval, so
			// to do a faster initial election, we have to commit to far more
			// frequent heartbeats (which are meaningless in a single-node
			// embedded cluster).
			//
			// So we crank the heartbeats to every 15ms if the data dir is
			// in $TMPDIR. The CPU overhead is minimal in tests and the
			// wall-time savings are huge.
			cfg.TickMs = 15
			cfg.ElectionMs = 75
		} else {
			cfg.TickMs = 50
			cfg.ElectionMs = 250
		}

		start := time.Now()
		e, err := embed.StartEtcd(cfg)
		if err != nil {
			return nil, fmt.Errorf("embedded server failed to start: %v", err)
		}
		db.embedClose = e.Close
		select {
		case <-e.Server.ReadyNotify():
		case <-ctx.Done():
			e.Server.Stop() // trigger a shutdown
			return nil, fmt.Errorf("embedded server took too long to start")
		}
		db.opts.Logf("etcd: embedded server started in %s (election timeout: %dms)", time.Since(start).Round(time.Microsecond), cfg.ElectionMs)
		eps = []string{"http://" + e.Clients[0].Addr().String()}
	} else {
		eps = strings.Split(urls, ",")
	}

	db.cli, err = clientv3.New(clientv3.Config{Endpoints: eps})
	if err != nil {
		return nil, fmt.Errorf("etcd.New: %v", err)
	}
	if opts.DeleteAllOnStart {
		_, err := db.cli.Delete(ctx, opts.KeyPrefix, clientv3.WithPrefix())
		if err != nil {
			db.cli.Close()
			return nil, err
		}
	}

	watchCh := db.cli.Watch(watchCtx, db.opts.KeyPrefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	firstWatchRes := <-watchCh // etcd watch sends a creation event
	if len(firstWatchRes.Events) > 0 {
		if err := db.watchResult(&firstWatchRes); err != nil {
			return nil, fmt.Errorf("first watch: %w", err)
		}
	}
	db.Mu.Lock()
	db.rev = rev(firstWatchRes.Header.Revision)
	db.Mu.Unlock()

	db.shutdownWG.Add(1)
	go func() {
		defer db.shutdownWG.Done()
		if err := db.watch(watchCh); err != nil {
			if watchCtx.Err() == nil {
				panic("etcd.watch: " + err.Error())
			}
			// otherwise, context was canceled so exit gracefully
			db.opts.Logf("etcd.watch: shutdown")
		}
	}()

	if err := db.loadAll(ctx); err != nil {
		return nil, fmt.Errorf("etcd.New: could not load: %w", err)
	}

	db.shutdownWG.Add(1)
	const watchdogMax = 300 * time.Second
	watchdogCh := syncs.Watch(watchCtx, &db.Mu, 30*time.Second, watchdogMax)
	go func() {
		defer db.shutdownWG.Done()
		for d := range watchdogCh {
			dbMuLockLatency.Set(int64(d))
			if d == watchdogMax {
				buf := new(strings.Builder)
				pprof.Lookup("goroutine").WriteTo(buf, 1)
				db.opts.Logf("etcd watchdog timeout stack:\n%s", buf.String())
				log.Fatalf("etcd watchdog timeout")
			}
		}
	}()

	return db, nil
}

// ReadTx create a new read-only transaction.
func (db *DB) ReadTx() *Tx {
	return &Tx{ro: true, db: db}
}

// Tx creates a new database transaction.
func (db *DB) Tx(ctx context.Context) *Tx {
	return &Tx{ctx: ctx, db: db}
}

// Close cancels all transactions and releases all DB resources.
func (db *DB) Close() error {
	db.Mu.Lock()
	closing := db.closing
	db.closing = true
	db.Mu.Unlock()

	if closing {
		return errors.New("etcd.DB: close already called")
	}

	db.watchCancel()
	db.shutdownWG.Wait()

	var err error
	if db.cli != nil {
		err = db.cli.Close()
	}
	if db.embedClose != nil {
		db.embedClose()
	}
	return err
}

// A Tx is an etcd transaction.
//
// A Tx holds no resources besides some private memory, so there is
// no notion of closing a transaction or rolling back a transaction.
// A cheap etcd read can be done with:
//
//	found, err := db.ReadTx().Get(key, &val)
//
// Tx is not safe for concurrent access.
// For concurrency, create more transactions.
type Tx struct {
	// PendingUpdate, if not nil, is called on each Put.
	// It can be used by higher-level objects to keep an index
	// up-to-date on a transaction state.
	//
	// The memory passed to PendingUpdate are only valid for the
	// duration of the call and must not be modified.
	//
	// A nil new value means the key has been deleted.
	PendingUpdate func(key string, old, new interface{})

	// Err is any error reported by the transaction during use.
	// This can be set used externally to ensure Commit does not fire.
	//
	// Once Err is set all future calls to Tx methods will return Err.
	// On Commit, if Err is not set, it is set to ErrTxClosed
	Err error

	ctx context.Context
	db  *DB
	ro  bool // readonly

	// maxRev is the maximum revision of this transaction.
	// If any key read-from or written-to has a greater rev,
	// then this Tx will fail with ErrTxStale.
	maxRev rev

	// cmps are keys that was read or written by this tx.
	// Tracked so on Commit they can be reported to etcd
	// to make sure they didn't change rev.
	cmps map[string]struct{}

	// puts are key-values written by this tx.
	// The value is cloned before being placed in this map
	// so the tx owns the memory (and can pass ownership onto
	// the db.cache on commit).
	puts map[string]interface{}
}

// Get retrieves a key-value from the etcd cache into value.
//
// The value must be a pointer to the decoded type of the key, or nil.
// The caller owns the returned value.
//
// No network events are generated.
//
// The first call to Get in a Tx will pin the global revision number
// to the current etcd revision. If a subsequent Get finds a value
// with a greater revision then get will return ErrTxStale.
// This ensures that a Tx has a consistent view of the values it fetches.
func (tx *Tx) Get(key string, value interface{}) (found bool, err error) {
	if tx.Err != nil {
		return false, tx.Err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("etcd.Get(%q): %w", key, err)
			tx.Err = err
		}
	}()

	found, kv, err := tx.get(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if value != nil {
		if err := tx.db.opts.CloneFunc(value, key, kv.value); err != nil {
			return false, err
		}
	}
	return true, nil
}

// UnsafePeek lets the caller see the cached value for a key.
//
// It is vital that the caller does not modify the value, or the DB will
// be corrupted.
func (tx *Tx) UnsafePeek(key string, peekFunc func(v interface{})) (found bool, err error) {
	if tx.Err != nil {
		return false, tx.Err
	}
	found, kv, err := tx.get(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	peekFunc(kv.value)
	return true, nil
}

// GetRange gets a range of KV-pairs from the etcd cache.
//
// The parameter fn is called with batches of matching KV-pairs.
// The passed slice and all the memory it references is owned by fn.
// If fn returns an error then GetRange aborts early and returns the error.
//
// While fn is called GetRange holds either the DB read or write lock,
// so no transactions can be committed from inside the fn callback.
//
// It is possible for the same key to be sent to fn more than once in a
// GetRange call. If this happens, the later key-value pair is a newer
// version that replaces the old value.
//
// When all keys have been read, finalFn is called holding the DB write lock.
// This gives the caller a chance to do something knowing that no key updates
// can happen between reading the range and executing finalFn.
func (db *DB) GetRange(keyPrefix string, fn func([]KV) error, finalFn func()) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("etcd.GetRange(%q): %w", keyPrefix, err)
		}
	}()

	// First take a read lock and send all relevant KV-pairs to fn.
	// This should include almost all of the KV-space.
	var revDone rev
	func() {
		db.Mu.RLock()
		defer db.Mu.RUnlock()
		revDone = db.rev
		err = db.getRange(keyPrefix, fn, 0)
	}()

	if err != nil {
		return err
	}

	// Now grab a write lock. Find all KV-pairs that have changed since
	// we held the read lock and send those to fn.
	//
	// The double pass is to minimize the time GetRange holds the write lock.
	db.Mu.Lock()
	defer db.Mu.Unlock()
	err = db.getRange(keyPrefix, fn, revDone+1)
	if err == nil && finalFn != nil {
		finalFn()
	}

	return err
}

// getRange gets all key-values with keyPrefix and passes them to fn.
//
// The DB read lock must be held for the duration of the call.
//
// TODO(crawshaw): This is an inefficient O(N) implementation.
// We can make this in-memory efficient by storing an ordered tree of keys,
// e.g. https://pkg.go.dev/github.com/dghubble/trie?tab=doc#PathTrie
// Or we can factor out the db.load method and use etcd's /range with keys_only=true.
func (db *DB) getRange(keyPrefix string, fn func([]KV) error, min rev) error {
	const window = 256
	var kvs []KV

	for key, kv := range db.cache {
		if !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		if kv.modRev < min {
			continue
		}
		if kv.value == nil {
			continue // deleted key
		}
		cloned, err := db.clone(key, kv.value)
		if err != nil {
			return fmt.Errorf("clone %s: %w", key, err)
		}
		kvs = append(kvs, KV{Key: key, Value: cloned})
		if len(kvs) > window {
			if err := fn(kvs); err != nil {
				return err
			}
			kvs = nil // passing ownership of kvs to fn
		}
	}
	if len(kvs) > 0 {
		return fn(kvs)
	}
	return nil
}

// Put adds or replaces a KV-pair in the transaction.
// If a newer value for the key is in the DB this will return ErrTxStale.
// A nil value deletes the key.
func (tx *Tx) Put(key string, value interface{}) error {
	if tx.ro {
		err := fmt.Errorf("etcd.Put(%q) called on read-only transaction", key)
		if tx.Err == nil {
			tx.Err = err
		}
		return err
	}
	if tx.Err != nil {
		return tx.Err
	}
	_, curVal, err := tx.get(key)
	if err != nil {
		tx.Err = fmt.Errorf("etcd.Put(%q): %w", key, err)
		return tx.Err
	}
	if tx.puts == nil {
		tx.puts = make(map[string]interface{})
	}
	var cloned interface{}
	if value != nil {
		cloned, err = tx.db.clone(key, value)
		if err != nil {
			tx.Err = fmt.Errorf("etcd.Put(%q): %w", key, err)
			return tx.Err
		}
	}
	if tx.PendingUpdate != nil {
		tx.PendingUpdate(key, curVal.value, value)
	}
	tx.puts[key] = cloned
	return nil
}

// Commit commits the transaction to etcd.
// It is an error to call Commit on a read-only transaction.
func (tx *Tx) Commit() (err error) {
	if tx.Err != nil {
		return fmt.Errorf("etcd.Commit: %w", tx.Err)
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("etcd.Commit: %w", err)
		}
		if tx.Err == nil {
			if err != nil {
				tx.Err = err
			} else {
				tx.Err = ErrTxClosed
			}
		}
	}()
	if tx.ro {
		return errors.New("tx is read-only")
	}
	if len(tx.puts) == 0 {
		return nil
	}

	if atomic.LoadInt32(&tx.db.panicOnWrite) > 0 {
		panic("db.PanicOnWrite: db write detected")
	}

	if tx.db.inMemory {
		return tx.commitInMemory()
	}

	tx.db.Mu.RLock()
	if tx.db.closing {
		tx.db.Mu.RUnlock()
		return ErrTxClosed
	}
	tx.db.shutdownWG.Add(1)
	tx.db.Mu.RUnlock()

	defer tx.db.shutdownWG.Done()
	ctx, cancel := context.WithCancel(tx.ctx)
	defer cancel()
	go func() {
		select {
		case <-tx.db.done:
			// db.Close called, cancel commit
		case <-ctx.Done():
			// tx.Commit complete or canceled, clean up this goroutine
		}
		cancel()
	}()

	var cmps []clientv3.Cmp
	for key := range tx.cmps {
		// Here we set the required mod revision of every key
		// we ever fetched in the transaction, and require it
		// not to have changed since we started.
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "<", int64(tx.maxRev+1)))
	}
	var puts []clientv3.Op
	for key, val := range tx.puts {
		if val == nil {
			puts = append(puts, clientv3.OpDelete(key))
			continue
		}
		data, err := tx.db.opts.EncodeFunc(key, val)
		if err != nil {
			return fmt.Errorf("EncodeFunc failed for key %q: %v", key, err)
		}
		puts = append(puts, clientv3.OpPut(key, string(data)))
	}

	txn := tx.db.cli.Txn(ctx)
	txn = txn.If(cmps...).Then(puts...)
	txnRes, err := txn.Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		if len(tx.puts) == 1 {
			var key string
			for k := range tx.puts {
				key = k
			}
			return fmt.Errorf("%w: key %s", ErrTxStale, key)
		}
		return ErrTxStale
	}

	txRev := rev(txnRes.Header.Revision)
	var done chan struct{}

	tx.db.Mu.Lock()
	// TODO(crawshaw): a potential optimization here is to put our
	// unaliased, ready to use value objects directly into the cache,
	// saving an encode/decode round-trip to the database.
	//
	// However there is one significant hurdle: once we put the rev
	// into the cache, we must increment tx.db.rev or a new Tx that
	// attempts to read the value will immediately fail with ErrTxStale.
	// But we cannot increment db.rev yet, as there may commits created
	// by other clients pending in the server that will come in later.
	//
	// So instead we must put the objects aside in a limbo, and add
	// them to the db.cache in watchResult. We must do this especially
	// carefully, as etcd may have chosen to amalgamate our commit with
	// some other client's commit, so the incoming txRev may include
	// more objects than we committed here. Either way, it is unsafe
	// to simply call:
	//
	// tx.commitCacheLocked(txRev)
	//
	// This optimization is significant and we should do it.
	// It should be easy enough to add a db.pendingCache and extract
	// the values from it in watchResult.
	if tx.db.rev < txRev {
		done = make(chan struct{})
		tx.db.pending[txRev] = append(tx.db.pending[txRev], done)
	}
	tx.db.Mu.Unlock()

	// Ensure the background watch has caught up to this commit, so that the
	// db revision is at or beyond this commit. This means sequential
	// commits will always see this one.
	if done != nil {
		<-done
	}

	return nil
}

// commitCacheLocked pushes the contents of tx into the DB cache.
// db.Mu must be held to call.
func (tx *Tx) commitCacheLocked(modRev rev) {
	// Immediately load the new values into the cache.
	// The watch will fill in these values shortly if it hasn't already
	// (and Tx.Commit will wait for it) but doing it here avoids having
	// to call DecodeFunc in the watch goroutine.
	var kvs []KV
	for key, val := range tx.puts {
		kv, exists := tx.db.cache[key]
		if exists && modRev <= kv.modRev {
			continue
		}
		tx.db.cache[key] = valueRev{
			value:  val,
			modRev: modRev,
		}
		if tx.db.hasWatchLocked() {
			var cloned interface{}
			if val != nil {
				var err error
				cloned, err = tx.db.clone(key, val)
				if err != nil {
					// By this point, we know val is the output of CloneFunc
					// called earlier in Tx.Put. That CloneFunc fails on
					// the value's second pass through suggests a bug in
					// the implementation of CloneFunc.
					panic(fmt.Sprintf("etcd tx watch second clone of %q failed: %v", key, err))
				}
			}
			kvs = append(kvs, KV{Key: key, OldValue: kv.value, Value: cloned})
		}
	}
	if tx.db.hasWatchLocked() && len(kvs) > 0 {
		tx.db.informWatchLocked(kvs)
	}
}

func (tx *Tx) commitInMemory() error {
	tx.db.Mu.Lock()
	defer tx.db.Mu.Unlock()

	// Check to make sure no other Tx beat us to the punch.
	for key := range tx.puts {
		kv, exists := tx.db.cache[key]
		if !exists {
			if tx.puts[key] == nil {
				delete(tx.puts, key) // do not process no-op deletes
			}
			continue
		}
		if kv.modRev > tx.maxRev {
			return fmt.Errorf("%w: key %s", ErrTxStale, key)
		}
		if tx.puts[key] == nil && kv.value == nil {
			delete(tx.puts, key) // do not process no-op deletes
		}
	}
	if len(tx.puts) == 0 {
		return nil
	}

	tx.db.rev++
	tx.commitCacheLocked(tx.db.rev)
	return nil
}

// valueRev is a decoded database value paired with its etcd mod revision.
type valueRev struct {
	value  interface{} // decoded value, nil if key was deleted
	modRev rev         // mod revision of this value
}

// rev is an etcd mod revision.
// The etcd server assigns a mod revision to every KV and to the whole database.
type rev int64

// watch issues a long-running watch request against etcd.
// Each transaction is received as a line of JSON.
func (db *DB) watch(ch clientv3.WatchChan) error {
	for res := range ch {
		if err := res.Err(); err != nil {
			return err
		}
		if err := db.watchResult(&res); err != nil {
			return err
		}
	}
	return fmt.Errorf("etcd.watch: [unexpected] watchchan closed")
}

// watchResult processes a JSON blob from the etcd watch API.
func (db *DB) watchResult(res *clientv3.WatchResponse) error {
	type newkv struct {
		key      string
		valueRev valueRev
	}
	var newkvs []newkv

	for _, ev := range res.Events {
		key := string(ev.Kv.Key)

		// As a first pass, we check the cache to see if we can avoid decoding
		// the value. This is a performance optimization, it's entirely possible
		// the Tx committing these values is still in-flight and will update the
		// db.cache momentarily, so it is checked again below under the mutex.
		db.Mu.RLock()
		kv, exists := db.cache[key]
		db.Mu.RUnlock()

		if exists && rev(ev.Kv.ModRevision) <= kv.modRev {
			// We already have this value.
			continue
		}

		if ev.Type == mvccpb.DELETE {
			newkvs = append(newkvs, newkv{
				key: key,
				valueRev: valueRev{
					modRev: rev(ev.Kv.ModRevision),
				},
			})
			continue
		}

		v, err := db.opts.DecodeFunc(key, ev.Kv.Value)
		if err != nil {
			panic(fmt.Sprintf("etcd.watch: bad decoded value for key %q: %v: %q", key, err, string(ev.Kv.Value)))
		}
		newkvs = append(newkvs, newkv{
			key: key,
			valueRev: valueRev{
				value:  v,
				modRev: rev(ev.Kv.ModRevision),
			},
		})
	}

	db.Mu.Lock()
	defer db.Mu.Unlock()

	var kvs []KV
	for _, newkv := range newkvs {
		kv, exists := db.cache[newkv.key]
		if exists && newkv.valueRev.modRev <= kv.modRev {
			// Value has just been updated by a Tx, keep newer value.
			continue
		}
		if !exists && newkv.valueRev.value == nil {
			// Value has been deleted but we never knew about it, ignore.
			continue
		}
		if db.hasWatchLocked() {
			var cloned interface{}
			if newkv.valueRev.value != nil {
				var err error
				cloned, err = db.clone(newkv.key, newkv.valueRev.value)
				if err != nil {
					panic(fmt.Sprintf("etcd.watch clone of %q failed: %v", newkv.key, err))
				}
			}
			kvs = append(kvs, KV{Key: newkv.key, OldValue: kv.value, Value: cloned})
		}
		db.cache[newkv.key] = newkv.valueRev
	}
	if db.hasWatchLocked() && len(kvs) > 0 {
		db.informWatchLocked(kvs)
	}
	db.rev = rev(res.Header.Revision)
	for rev, doneChs := range db.pending {
		if rev <= db.rev {
			for _, done := range doneChs {
				close(done)
			}
			delete(db.pending, rev)
		}
	}

	return nil
}

func (db *DB) clone(key string, val interface{}) (interface{}, error) {
	dst := reflect.New(reflect.TypeOf(val))
	if err := db.opts.CloneFunc(dst.Interface(), key, val); err != nil {
		return nil, err
	}
	return dst.Elem().Interface(), nil
}

// loadAll loads all the keys from etcd into DB using a series of
// paged range requests.
//
// The requests are not pinned to any version. To get a consistent view
// of the DB, a watcher must be started before loadAll is called.
func (db *DB) loadAll(ctx context.Context) error {
	const batchSize = 1000
	start := time.Now()
	db.opts.Logf("etcd.loadAll: loading all KVs with prefix %s", db.opts.KeyPrefix)
	opts := []clientv3.OpOption{
		clientv3.WithLimit(batchSize),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(db.opts.KeyPrefix)),
	}

	errCh := make(chan error)
	countCh := make(chan int)
	respSizeCh := make(chan int)

	batches := 0
	key := db.opts.KeyPrefix
	for {
		batches++
		resp, err := db.cli.Get(ctx, key, opts...)
		if err != nil {
			return err
		}
		go func() {
			count, err := db.load(resp)
			errCh <- err
			countCh <- count

			n := 0
			for _, kv := range resp.Kvs {
				n += len(kv.Value)
			}
			respSizeCh <- n
		}()
		if !resp.More {
			break
		}
		key = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
	db.opts.Logf("etcd.loadAll: KVs read from DB in %s", time.Since(start).Round(time.Millisecond))

	var err error
	loaded := 0
	maxRespSize := 0
	for i := 0; i < batches; i++ {
		if err2 := <-errCh; err == nil {
			err = err2
		}
		loaded += <-countCh
		if n := <-respSizeCh; n > maxRespSize {
			maxRespSize = n
		}
	}
	if err != nil {
		return err
	}

	db.opts.Logf("etcd.loadAll: %d KVs read and decoded in %s", loaded, time.Since(start).Round(time.Millisecond))
	db.opts.Logf("etcd.loadAll: %d batches read, largest batch was %d bytes", batches, maxRespSize)
	return nil
}

func (db *DB) load(resp *clientv3.GetResponse) (count int, err error) {
	var vals []interface{}
	for _, kv := range resp.Kvs {
		v, err := db.opts.DecodeFunc(string(kv.Key), kv.Value)
		if err != nil {
			return 0, fmt.Errorf("%q: cannot decode: %w", string(kv.Key), err)
		}
		vals = append(vals, v)
	}

	db.Mu.Lock()
	defer db.Mu.Unlock()

	var kvs []KV
	for i, kv := range resp.Kvs {
		key := string(kv.Key)
		if _, exists := db.cache[key]; exists {
			continue // skip keys already filled by watch
		}
		v := vals[i]
		db.cache[key] = valueRev{
			value:  v,
			modRev: rev(kv.ModRevision),
		}
		if db.opts.WatchFunc != nil {
			cloned, err := db.clone(key, v)
			if err != nil {
				return 0, fmt.Errorf("%q clone of decoded value failed: %w", key, err)
			}
			kvs = append(kvs, KV{Key: key, Value: cloned})
		}
	}

	if len(kvs) > 0 {
		db.opts.WatchFunc(kvs)
	}

	return len(kvs), nil
}

// PanicOnWrite sets whether the db should panic when a write is requested.
// It is used in tests to ensure that particular actions do not create db writes.
// Calls to PanicOnWrite may be nested.
func (db *DB) PanicOnWrite(enable bool) {
	if enable {
		atomic.AddInt32(&db.panicOnWrite, 1)
	} else {
		if atomic.AddInt32(&db.panicOnWrite, -1) < 0 {
			panic("db.PanicOnWrite underflow")
		}
	}
}

// UnsafeClient exposes the raw underlying etcd client used by the database.
// Use with extreme care.
func (db *DB) UnsafeClient() *clientv3.Client {
	return db.cli
}

func (tx *Tx) get(key string) (bool, valueRev, error) {
	if !strings.HasPrefix(key, tx.db.opts.KeyPrefix) {
		return false, valueRev{}, fmt.Errorf("key does not use prefix %s", tx.db.opts.KeyPrefix)
	}

	putValue, isPut := tx.puts[key]
	if isPut {
		if putValue == nil {
			return false, valueRev{}, nil
		}
		v, err := tx.db.clone(key, putValue)
		if err != nil {
			return false, valueRev{}, err
		}
		return true, valueRev{value: v, modRev: tx.maxRev}, nil
	}

	tx.db.Mu.RLock()
	kv, ok := tx.db.cache[key]
	if ok && tx.maxRev == 0 {
		tx.maxRev = tx.db.rev
		if kv.modRev > tx.maxRev {
			tx.db.Mu.RUnlock()
			panic(fmt.Sprintf("on new tx kv.modRev %d > tx.maxRev %d", kv.modRev, tx.maxRev))
		}
	}
	tx.db.Mu.RUnlock()

	if tx.maxRev < kv.modRev {
		return false, valueRev{}, ErrTxStale
	}
	if !ok || kv.value == nil {
		return false, valueRev{}, nil
	}
	if !tx.ro {
		if tx.cmps == nil {
			tx.cmps = make(map[string]struct{})
		}
		tx.cmps[key] = struct{}{}
	}
	return true, kv, nil
}

func (db *DB) hasWatchLocked() bool {
	return db.opts.WatchFunc != nil || len(db.keyWatchers) > 0 || len(db.prefixWatchers) > 0
}

func (db *DB) informWatchLocked(kvs []KV) {
	if len(kvs) == 0 {
		panic("informWatchLocked called with no KVs")
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	if db.opts.WatchFunc != nil {
		db.opts.WatchFunc(kvs)
	}

	runWatches := func(watches []watch, kvs []KV) []watch {
		if len(watches) == 0 {
			return nil
		}
		if len(watches) > 1 {
			rand.Shuffle(len(watches), func(i, j int) {
				watches[i], watches[j] = watches[j], watches[i]
			})
		}
		// TODO(crawshaw): implement compaction by swapping last w into
		// current spot, nilling out last function, decrementing i,
		// and shrinking the slice?
		newWatches := watches[:0]
		for _, w := range watches {
			if w.ctx.Err() != nil {
				continue
			}
			w.fn(kvs)
			if w.ctx.Err() != nil {
				continue
			}
			newWatches = append(newWatches, w)
		}
		tail := watches[len(newWatches):]
		for i := range tail {
			// Clear out old funcs so that the backing array
			// does not pin removed watch functions.
			tail[i] = watch{}
		}
		return newWatches
	}

	// Process key watchers.
	for _, kv := range kvs {
		db.keyWatchers[kv.Key] = runWatches(db.keyWatchers[kv.Key], []KV{kv})
	}

	// Process prefix watchers.
	deliveries := make(map[string][]KV)
	for _, kv := range kvs {
		for prefix := range db.prefixWatchers {
			if strings.HasPrefix(kv.Key, prefix) {
				deliveries[prefix] = append(deliveries[prefix], kv)
			}
		}
	}
	for prefix, kvs := range deliveries {
		db.prefixWatchers[prefix] = runWatches(db.prefixWatchers[prefix], kvs)
	}
}

type watch struct {
	ctx context.Context
	fn  func(kvs []KV)
}

// WatchKey registers fn to be called every time a change is made to a key.
//
// If there is an existing value of the key it is played through fn
// before WatchKey returns.
//
// The fn function is called holding either the read or write lock.
// Do not do any operation in fn that tries to take the etcd read or
// write lock.
//
// The watch is de-registered when ctx is Done.
func (db *DB) WatchKey(ctx context.Context, key string, fn func(old, new interface{})) error {
	w := watch{
		ctx: ctx,
		fn: func(kvs []KV) {
			if len(kvs) != 1 {
				panic(fmt.Sprintf("WatchKey callback receieved %d kvs", len(kvs)))
			}
			kv := kvs[0]
			if kv.Key != key {
				panic(fmt.Sprintf("WatchKey callback receieved wrong key %q, want %q", kv.Key, key))
			}
			fn(kv.OldValue, kv.Value)
		},
	}

	// Two passes. First try to send the current value holding the read lock.
	// This sometimes short-cuts the watch so no write lock is necessary.
	// This mostly lets us call fn under the read lock, for better
	// general concurrency.

	db.Mu.RLock()
	kv1, kv1ok := db.cache[key]
	if kv1ok {
		cloned, err := db.clone(key, kv1.value)
		if err != nil {
			db.Mu.RUnlock()
			return fmt.Errorf("WatchKey clone %s: %w", key, err)
		}
		fn(nil, cloned)
	}
	db.Mu.RUnlock()

	if ctx.Err() != nil {
		return nil // first value was enough, call it quits
	}

	db.Mu.Lock()
	defer db.Mu.Unlock()
	if kv2, ok := db.cache[key]; ok && kv2.modRev > kv1.modRev {
		// The value changed between the rlock and the wlock,
		// so send the newer value.
		var cloned1 interface{}
		if kv1ok {
			var err error
			cloned1, err = db.clone(key, kv1.value)
			if err != nil {
				return fmt.Errorf("WatchKey clone %s: %w", key, err)
			}
		}
		cloned2, err := db.clone(key, kv2.value)
		if err != nil {
			return fmt.Errorf("WatchKey clone %s: %w", key, err)
		}
		fn(cloned1, cloned2)
	}
	db.keyWatchers[key] = append(db.keyWatchers[key], w)
	return nil
}

// WatchPrefix registers fn to be called every time a change is made to
// a key-value with the prefix keyPrefix.
//
// All existing key-values matching keyPrefix are played through fn
// before WatchPrefix returns.
//
// The fn function is called holding either the read or write lock.
// Do not do any operation in fn that tries to take the etcd read or
// write lock.
//
// The watch is de-registered when ctx is Done.
//
// The data structure storing prefix watchers is relatively inefficient
// at present, so adding large numbers of prefix watchers is expensive.
func (db *DB) WatchPrefix(ctx context.Context, keyPrefix string, fn func(kvs []KV)) error {
	// errNoContinue is used internally to deregister watch functions
	errNoContinue := errors.New("watch does not continue")

	// TODO: reorganize this to more aggressively end the watch when
	// ctx is done. (Probably by changing GetRange to use a ctx.)

	// Run all existing key-values through fn.
	fnRange := func(kv []KV) error {
		if ctx.Err() != nil {
			return errNoContinue
		}
		fn(kv)
		return nil
	}
	onSuccess := func() {
		// etcd.Mu write lock is held by GetRange
		db.prefixWatchers[keyPrefix] = append(db.prefixWatchers[keyPrefix], watch{
			ctx: ctx,
			fn:  fn,
		})
	}
	err := db.GetRange(keyPrefix, fnRange, onSuccess)
	if err != nil && !errors.Is(err, errNoContinue) {
		return fmt.Errorf("cfgdb.AddWatch: %w", err)
	}
	return nil

}

// TODO(crawshaw): type Key string ?
// TODO(crawshaw): Delete
// TODO(crawshaw): Watch Delete
