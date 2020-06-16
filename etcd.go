// Package etcd implements an etcd v3 client.
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
// Everything else should follow a programmer's intution for an
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
// Implementation Notes
//
// Built on the gRPC JSON gateway:
// https://etcd.io/docs/v3.4.0/dev-guide/api_grpc_gateway
// This costs us in throughput and latency to etcd, while keeping gRPC
// (and its ops overhead) out of our software.
//
// As the REST API is generated from the gRPC API, this is the
// canonical source for figuring out commands:
// https://github.com/etcd-io/etcd/blob/master/etcdserver/etcdserverpb/rpc.proto
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
package etcd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrTxStale is reported when another transaction has modified a key
// referenced by this transaction, so it can no longer be applied to
// the database.
var ErrTxStale = errors.New("tx stale")

// ErrTxClosed is reported when a method is called on a committed or
// canceled Tx.
var ErrTxClosed = errors.New("tx closed")

// DB is a read-write datastore backed by etcd.
type DB struct {
	url      string
	opts     Options
	inMemory bool // entirely in-memory

	done        <-chan struct{}
	watchCancel func()
	shutdownWG  sync.WaitGroup // shutdownWG.Add is called under mu when !closing

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

	cache   map[string]valueRev     // in-memory copy of all etcd key-values
	rev     rev                     // rev is the latest known etcd db revision
	closing bool                    // DB.Close called
	pending map[rev][]chan struct{} // channels to be closed when rev >= map key
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
	// The DB.Mu write lock is held for the call, so no transcations
	// can be issued from inside WatchFunc.
	//
	// Entire etcd transactions are single calls to WatchFunc.
	//
	// The called WatchFunc owns the values passed to it.
	WatchFunc func([]KV)
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
	OldValue interface{}
	Value    interface{}
}

// New loads the contents of an etcd prefix range and creates a *DB
// for reading and writing from the prefix range.
//
// The url value is the etcd JSON HTTP endpoint, e.g. "http://localhost:2379".
//
// As a special case, the url may be "memory://".
// In this mode, the DB does not connect to any etcd server, instead all
// operations are performed on the in-memory cache.
func New(ctx context.Context, url string, opts Options) (*DB, error) {
	opts, err := opts.fillDefaults()
	if err != nil {
		return nil, err
	}
	db := &DB{
		url:     url,
		opts:    opts,
		cache:   map[string]valueRev{},
		pending: map[rev][]chan struct{}{},
	}
	if db.url == "memory://" {
		db.inMemory = true
	} else {
		if err := db.loadAll(ctx); err != nil {
			return nil, fmt.Errorf("etcd.New: could not load: %w", err)
		}
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	db.done = watchCtx.Done()
	db.shutdownWG.Add(1)
	db.watchCancel = cancel
	go db.watchRoutine(watchCtx)

	return db, nil
}

func (db *DB) watchRoutine(ctx context.Context) {
	defer db.shutdownWG.Done()
	if db.inMemory {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if err := db.watch(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			db.opts.Logf("etcd.watch: %v", err)
		}
		// TODO(crawshaw): the obvious thing to do here, backoff,
		// isn't what we want in practice unless we design the server
		// to handle being disconnected from the DB. What to do?
		t := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
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
	return nil
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

// GetRange gets a range of KV-pairs from the etcd cache.
//
// The parameter fn is called with batches of matching KV-pairs.
// The passed slice and all the memory it references is owned by fn.
// If fn returns an error then GetRange aborts early and returns the error.
//
// For the duration of the GetRange call a DB-wide lock is held,
// so no transactions can be committed from inside the fn callback.
func (tx *Tx) GetRange(keyPrefix string, fn func([]KV) error) (err error) {
	if tx.Err != nil {
		return tx.Err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("etcd.GetRange(%q): %w", keyPrefix, err)
			if tx.Err == nil {
				tx.Err = err
			}
		}
	}()

	var kvs []KV

	// TODO(crawshaw): This is an inefficient O(N) implementation.
	// We can make this in-memory efficient by storing an ordered tree of keys,
	// e.g. https://pkg.go.dev/github.com/dghubble/trie?tab=doc#PathTrie
	// Or we can factor out the db.load method and use etcd's /range with keys_only=true.
	tx.db.Mu.RLock()
	defer tx.db.Mu.RUnlock()
	for key := range tx.db.cache {
		if !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		found, kv, err := tx.get(key)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		} else if !found {
			return fmt.Errorf("%s: cannot find expected key", key)
		}
		cloned, err := tx.db.clone(key, kv.value)
		if err != nil {
			return fmt.Errorf("clone %s: %w", key, err)
		}
		kvs = append(kvs, KV{Key: key, Value: cloned})
		if len(kvs) > 100 {
			if err := fn(kvs); err != nil {
				return err
			}
			kvs = nil
		}
	}
	if len(kvs) > 0 {
		if err := fn(kvs); err != nil {
			return err
		}
		kvs = nil // passing ownership of kvs to fn
	}
	return err
}

// Put adds or replaces a KV-pair in the transaction.
// If a newer value for the key is in the DB this will return ErrTxStale.
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
	cloned, err := tx.db.clone(key, value)
	if err != nil {
		tx.Err = fmt.Errorf("etcd.Put(%q): %w", key, err)
		return tx.Err
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

	// Here we build the transaction request to etcd.
	// There are some light examples of this API here:
	//
	//	https://etcd.io/docs/v3.4.0/dev-guide/api_grpc_gateway/
	//
	// The canonical definition of this interface is the underlying
	// protos that are being mechanically converted to JSON:
	//
	//	https://github.com/etcd-io/etcd/blob/master/etcdserver/etcdserverpb/rpc.proto#L606
	//
	type txnCompare struct { // message Compare
		ModRevision rev    `json:"mod_revision"`
		Result      string `json:"result"`
		Target      string `json:"target"`
		Key         []byte `json:"key"`
	}
	type txnSuccess struct { // message RequestOp
		RequestPut struct {
			Key   []byte `json:"key"`
			Value []byte `json:"value"`
		} `json:"requestPut"`
	}
	var txnReq struct { // message TxnRequest
		Compare []txnCompare `json:"compare"` // conditions
		Success []txnSuccess `json:"success"` // actions if conditions met
	}
	for key := range tx.cmps {
		// Here we set the required mod revision of every key
		// we ever fetched in the transaction, and require it
		// not to have changed since we started.
		txnReq.Compare = append(txnReq.Compare, txnCompare{
			ModRevision: tx.maxRev + 1,
			Result:      "LESS",
			Target:      "MOD",
			Key:         []byte(key),
		})
	}
	for key, val := range tx.puts {
		data, err := tx.db.opts.EncodeFunc(key, val)
		if err != nil {
			return fmt.Errorf("EncodeFunc failed for key %q: %v", key, err)
		}

		var s txnSuccess
		s.RequestPut.Key = []byte(key)
		s.RequestPut.Value = data
		txnReq.Success = append(txnReq.Success, s)
	}
	data, err := json.Marshal(txnReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", tx.db.url+"/v3/kv/txn", bytes.NewReader(data))
	if err != nil {
		return err
	}
	if tx.db.opts.AuthHeader != "" {
		req.Header.Set("Authorization", tx.db.opts.AuthHeader)
	}
	req = req.WithContext(ctx)
	res, err := tx.db.opts.HTTPC.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := ioutil.ReadAll(res.Body)
		str := string(b)
		if res.StatusCode == 400 && strings.Contains(str, "too many operations") {
			builder := new(strings.Builder)
			fmt.Fprintf(builder, "cmps (%d):", len(tx.cmps))
			for key := range tx.cmps {
				fmt.Fprintf(builder, "\n\t%s", key)
			}
			fmt.Fprintf(builder, "\nputs (%d):", len(tx.puts))
			for _, s := range txnReq.Success {
				fmt.Fprintf(builder, "\n\t%s: %s", s.RequestPut.Key, s.RequestPut.Value)
			}
			return fmt.Errorf("too many operations: %s", builder)
		}
		return fmt.Errorf("status=%d: %q", res.StatusCode, str)
	}
	var txnRes struct { // message TxnResponse
		Header struct {
			Revision rev `json:"revision"`
		} `json:"header"`
		Succeeded bool `json:"succeeded"`
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response: %w: %s", err, b)
	}
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&txnRes); err != nil {
		return fmt.Errorf("decode response: %w: %v", err, b)
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

	txRev := txnRes.Header.Revision
	var done chan struct{}

	tx.db.Mu.Lock()
	tx.commitCacheLocked(txRev)
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
		if tx.db.opts.WatchFunc != nil {
			cloned, err := tx.db.clone(key, val)
			if err != nil {
				// By this point, we know val is the output of CloneFunc
				// called earlier in Tx.Put. That CloneFunc fails on
				// the value's second pass through suggests a bug in
				// the implementation of CloneFunc.
				panic(fmt.Sprintf("etcd tx watch second clone of %q failed: %v", key, err))
			}
			kvs = append(kvs, KV{Key: key, OldValue: kv.value, Value: cloned})
		}
	}
	if tx.db.opts.WatchFunc != nil && len(kvs) > 0 {
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		tx.db.opts.WatchFunc(kvs)
	}
}

func (tx *Tx) commitInMemory() error {
	tx.db.Mu.Lock()
	defer tx.db.Mu.Unlock()

	// Check to make sure no other Tx beat us to the punch.
	for key := range tx.puts {
		kv, exists := tx.db.cache[key]
		if !exists {
			continue
		}
		if kv.modRev > tx.maxRev {
			return fmt.Errorf("%w: key %s", ErrTxStale, key)
		}
	}

	tx.db.rev++
	tx.commitCacheLocked(tx.db.rev)
	return nil
}

// valueRev is a decoded database value paired with its etcd mod revision.
type valueRev struct {
	value  interface{} // decoded value
	modRev rev         // mod revision of this value
}

// rev is an etcd mod revision.
// The etcd server assigns a mod revision to every KV and to the whole database.
type rev int64

func (r rev) MarshalText() (text []byte, err error) {
	return []byte(fmt.Sprintf("%d", int64(r))), nil
}
func (r *rev) UnmarshalText(text []byte) error {
	_, err := fmt.Sscanf(string(text), "%d", (*int64)(r))
	return err
}

// watch issues a long-running watch request against etcd.
// Each transaction is received es a line of JSON.
func (db *DB) watch(ctx context.Context) error {
	var watchRequest struct {
		CreateRequest struct {
			Key           []byte `json:"key"`
			RangeEnd      []byte `json:"range_end"`
			StartRevision int64  `json:"start_revision"`
		} `json:"create_request"`
	}
	watchRequest.CreateRequest.Key = []byte(db.opts.KeyPrefix)
	watchRequest.CreateRequest.RangeEnd = addOne([]byte(db.opts.KeyPrefix))

	db.Mu.RLock()
	watchRequest.CreateRequest.StartRevision = int64(db.rev)
	db.Mu.RUnlock()

	data, err := json.Marshal(watchRequest)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", db.url+"/v3/watch", bytes.NewReader(data))
	if err != nil {
		return err
	}
	if db.opts.AuthHeader != "" {
		req.Header.Set("Authorization", db.opts.AuthHeader)
	}
	req = req.WithContext(ctx)
	res, err := db.opts.HTTPC.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("status=%d: %q", res.StatusCode, string(b))
	}

	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		if err := db.watchResult(scanner.Bytes()); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// watchResult processes a JSON blob from the etcd watch API.
func (db *DB) watchResult(data []byte) error {
	var watchResult struct {
		Result struct {
			Header struct {
				Revision rev `json:"revision"`
			} `json:"header"`
			Events []struct {
				Type string `json:"type"`
				KV   struct {
					Key         []byte `json:"key"`
					ModRevision rev    `json:"mod_revision"`
					Value       []byte `json:"value"`
				} `json:"kv"`
			} `json:"events"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &watchResult); err != nil {
		return err
	}

	type newkv struct {
		key      string
		valueRev valueRev
	}
	var newkvs []newkv

	for _, ev := range watchResult.Result.Events {
		key := string(ev.KV.Key)

		// As a first pass, we check the cache to see if we can avoid decoding
		// the value. This is a performance optimization, it's entirely possible
		// the Tx commiting these values is still in-flight and will update the
		// db.cache momentarily, so it is checked again below under the mutex.
		db.Mu.RLock()
		kv, exists := db.cache[key]
		db.Mu.RUnlock()

		if exists && ev.KV.ModRevision <= kv.modRev {
			// We already have this value.
			continue
		}

		if ev.Type == "DELETE" {
			db.opts.Logf("etcd.watch: TODO delete key %s", ev.KV.Key)
			continue
		}

		v, err := db.opts.DecodeFunc(key, ev.KV.Value)
		if err != nil {
			panic(fmt.Sprintf("etcd.watch: bad decoded value for key %q: %v: %q", key, err, string(ev.KV.Value)))
		}
		newkvs = append(newkvs, newkv{
			key: key,
			valueRev: valueRev{
				value:  v,
				modRev: ev.KV.ModRevision,
			},
		})
	}

	db.Mu.Lock()
	var kvs []KV
	for _, newkv := range newkvs {
		kv, exists := db.cache[newkv.key]
		if exists && newkv.valueRev.modRev <= kv.modRev {
			// Value has just been updated by a Tx, keep newer value.
			continue
		}
		if db.opts.WatchFunc != nil {
			cloned, err := db.clone(newkv.key, newkv.valueRev.value)
			if err != nil {
				panic(fmt.Sprintf("etcd.watch clone of %q failed: %v", newkv.key, err))
			}
			kvs = append(kvs, KV{Key: newkv.key, OldValue: kv.value, Value: cloned})
		}
		db.cache[newkv.key] = newkv.valueRev
	}
	if len(kvs) > 0 {
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		db.opts.WatchFunc(kvs)
	}
	db.rev = rev(watchResult.Result.Header.Revision)
	for rev, doneChs := range db.pending {
		if rev <= db.rev {
			for _, done := range doneChs {
				close(done)
			}
			delete(db.pending, rev)
		}
	}
	db.Mu.Unlock()

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
// The requests are pinned at a specific db revision number to ensure the
// final view of the database is consistent. The watchRoutine then starts
// requesting keys at the revision we pinned the load at, so that all
// changes that happen during the load are correctly played into DB.
func (db *DB) loadAll(ctx context.Context) error {
	db.Mu.Lock()
	defer db.Mu.Unlock()

	startKey := []byte(db.opts.KeyPrefix)
	endKey := addOne([]byte(db.opts.KeyPrefix))
	var maxModRev rev
	for len(startKey) > 0 {
		nextKey, dbRev, err := db.load(ctx, startKey, endKey, maxModRev)
		if err != nil {
			return err
		}
		if maxModRev == 0 {
			maxModRev = dbRev
		}
		startKey = nextKey
	}
	db.rev = maxModRev
	return nil
}

const loadPageLimit = 100

// load issues a range request against etcd for the range startKey-endKey
// and processes up to loadPageLimit keys.
//
// keys versions are pinned at maxModRev (0 means the current db revision).
// This can be used to ensure a range request spread across several calls
// to load gets a consistent view of the database.
//
// When load returns it reports the next key in the cursor and the db
// revision the range request was issued at.
func (db *DB) load(ctx context.Context, startKey, endKey []byte, maxModRev rev) (nextKey []byte, dbRev rev, err error) {
	var keyRange struct {
		Key            []byte `json:"key"`
		RangeEnd       []byte `json:"range_end,omitempty"`
		Limit          int    `json:"limit,omitempty"`
		MaxModRevision int64  `json:"max_mod_revision,omitempty"`
	}
	keyRange.Key = startKey
	keyRange.RangeEnd = endKey
	keyRange.Limit = loadPageLimit
	keyRange.MaxModRevision = int64(maxModRev)
	keyRangeData, err := json.Marshal(keyRange)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest("POST", db.url+"/v3/kv/range", bytes.NewReader(keyRangeData))
	if err != nil {
		return nil, 0, err
	}
	if db.opts.AuthHeader != "" {
		req.Header.Set("Authorization", db.opts.AuthHeader)
	}
	req = req.WithContext(ctx)
	res, err := db.opts.HTTPC.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := ioutil.ReadAll(res.Body)
		return nil, 0, fmt.Errorf("etcd.New: key range status=%d: %q", res.StatusCode, string(b))
	}

	var rangeResponse struct {
		Header struct {
			Revision rev `json:"revision"`
		} `json:"header"`
		KVs []struct {
			ModRevision rev    `json:"mod_revision"`
			Key         []byte `json:"key"`
			Value       []byte `json:"value"`
		} `json:"kvs"`
		More bool `json:"more"`
	}
	if err := json.NewDecoder(res.Body).Decode(&rangeResponse); err != nil {
		return nil, 0, err
	}
	dbRev = rangeResponse.Header.Revision

	var kvs []KV
	for _, kv := range rangeResponse.KVs {
		key := string(kv.Key)
		v, err := db.opts.DecodeFunc(key, kv.Value)
		if err != nil {
			return nil, 0, fmt.Errorf("%q: cannot decode: %w", key, err)
		}
		db.cache[key] = valueRev{
			value:  v,
			modRev: kv.ModRevision,
		}
		if db.opts.WatchFunc != nil {
			cloned, err := db.clone(key, v)
			if err != nil {
				return nil, 0, fmt.Errorf("%q clone of decoded value failed: %w", key, err)
			}
			kvs = append(kvs, KV{Key: key, Value: cloned})
		}
	}
	if len(kvs) > 0 {
		db.opts.WatchFunc(kvs)
	}

	if rangeResponse.More {
		nextKey = addOne([]byte(rangeResponse.KVs[len(rangeResponse.KVs)-1].Key))
	}
	return nextKey, dbRev, nil
}

// addOne modifies v to be the next key in lexicographic order.
func addOne(v []byte) []byte {
	for len(v) > 0 && v[len(v)-1] == 0xff {
		v = v[:len(v)-1]
	}
	if len(v) > 0 {
		v[len(v)-1]++
	}
	return v
}

func (tx *Tx) get(key string) (bool, valueRev, error) {
	if !strings.HasPrefix(key, tx.db.opts.KeyPrefix) {
		return false, valueRev{}, fmt.Errorf("key does not use prefix %s", tx.db.opts.KeyPrefix)
	}

	putValue, isPut := tx.puts[key]
	if isPut {
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
	}
	tx.db.Mu.RUnlock()

	if !ok {
		return false, valueRev{}, nil
	}
	if tx.maxRev < kv.modRev {
		return false, valueRev{}, ErrTxStale
	}
	if tx.cmps == nil {
		tx.cmps = make(map[string]struct{})
	}
	tx.cmps[key] = struct{}{}
	return true, kv, nil
}

// TODO(crawshaw): type Key string ?
// TODO(crawshaw): Delete
// TODO(crawshaw): Watch Delete
