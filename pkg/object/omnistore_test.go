/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	omnistore "omnistorage/libomnistore/go"
)

type fakeOmniStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newFakeOmniStore() *fakeOmniStore {
	return &fakeOmniStore{data: make(map[string][]byte)}
}

func fakeKey(ref omnistore.ChunkRef) string {
	return strconv.FormatUint(ref.PoolID, 10) + ":" + ref.ChunkID
}

func (f *fakeOmniStore) ReadAt(_ context.Context, ref omnistore.ChunkRef, off uint32, p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.data[fakeKey(ref)]
	if !ok {
		return 0, omnistore.ErrNotFound
	}
	if int(off) >= len(data) {
		return 0, io.EOF
	}
	return copy(p, data[off:]), nil
}

func (f *fakeOmniStore) WriteAt(_ context.Context, ref omnistore.ChunkRef, off uint32, p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fakeKey(ref)
	data := f.data[key]
	end := int(off) + len(p)
	if end > int(omnistore.MaxChunkSize) {
		return 0, omnistore.ErrOutOfRange
	}
	if len(data) < end {
		data = append(data, make([]byte, end-len(data))...)
	}
	copy(data[off:], p)
	f.data[key] = data
	return len(p), nil
}

func (f *fakeOmniStore) Stat(_ context.Context, ref omnistore.ChunkRef) (omnistore.ChunkInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.data[fakeKey(ref)]
	if !ok {
		return omnistore.ChunkInfo{}, omnistore.ErrNotFound
	}
	return omnistore.ChunkInfo{Size: uint32(len(data))}, nil
}

func (f *fakeOmniStore) Delete(_ context.Context, ref omnistore.ChunkRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fakeKey(ref)
	if _, ok := f.data[key]; !ok {
		return omnistore.ErrNotFound
	}
	delete(f.data, key)
	return nil
}

func (f *fakeOmniStore) ListChunks(_ context.Context, poolID uint64, prefix, marker string, limit uint32) ([]omnistore.ChunkEntry, bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	poolPrefix := strconv.FormatUint(poolID, 10) + ":"
	keys := make([]string, 0)
	for key := range f.data {
		if !strings.HasPrefix(key, poolPrefix) {
			continue
		}
		objectID := strings.TrimPrefix(key, poolPrefix)
		if !strings.HasPrefix(objectID, prefix) || (marker != "" && objectID <= marker) {
			continue
		}
		keys = append(keys, objectID)
	}
	sort.Strings(keys)
	if limit == 0 {
		return nil, false, "", nil
	}
	hasMore := uint64(len(keys)) > uint64(limit)
	if hasMore {
		keys = keys[:limit]
	}
	entries := make([]omnistore.ChunkEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, omnistore.ChunkEntry{ChunkID: key, Size: uint32(len(f.data[poolPrefix+key]))})
	}
	next := ""
	if hasMore && len(entries) > 0 {
		next = entries[len(entries)-1].ChunkID
	}
	return entries, hasMore, next, nil
}

func (f *fakeOmniStore) Close() error { return nil }

func TestParseOmniStoreEndpoint(t *testing.T) {
	parsed, err := parseOmniStoreEndpoint("omnistore://pool/sync-prefix?pool-id=7&mon=mon-a:3333,mon-b:3333&mon=mon-c:3333")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.poolID != 7 {
		t.Fatalf("pool id: got %d, want 7", parsed.poolID)
	}
	if got, want := len(parsed.monitors), 3; got != want {
		t.Fatalf("monitor count: got %d, want %d", got, want)
	}
	if parsed.prefix != "sync-prefix/" {
		t.Fatalf("prefix: got %q, want %q", parsed.prefix, "sync-prefix/")
	}

	if _, err := parseOmniStoreEndpoint("omnistore://pool?mon=mon-a:3333"); err == nil {
		t.Fatal("expected missing pool-id error")
	}
	if _, err := parseOmniStoreEndpoint("omnistore://pool?pool-id=7"); err == nil {
		t.Fatal("expected missing monitor error")
	}
}

func TestOmniStorePrefixScopesKeys(t *testing.T) {
	client := newFakeOmniStore()
	first := &omnistoreStorage{client: client, poolID: 1, prefix: "first/"}
	second := &omnistoreStorage{client: client, poolID: 1, prefix: "second/"}
	ctx := context.Background()

	if err := first.Put(ctx, "same", bytes.NewReader([]byte("first"))); err != nil {
		t.Fatal(err)
	}
	if err := second.Put(ctx, "same", bytes.NewReader([]byte("second"))); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.data[fakeKey(omnistore.ChunkRef{PoolID: 1, ChunkID: "first/same"})]; !ok {
		t.Fatal("first prefix was not applied to the physical key")
	}
	if got := string(readOmniObject(t, first, "same", 0, -1)); got != "first" {
		t.Fatalf("first prefix read: got %q", got)
	}
	if got := string(readOmniObject(t, second, "same", 0, -1)); got != "second" {
		t.Fatalf("second prefix read: got %q", got)
	}

	objects, more, next, err := first.List(ctx, "", "", "", "", 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if more || next != "" || len(objects) != 1 || objects[0].Key() != "same" {
		t.Fatalf("scoped list: objects=%v more=%v next=%q", objects, more, next)
	}
}

func TestOmniStoreObjectOperations(t *testing.T) {
	store := &omnistoreStorage{
		client: newFakeOmniStore(),
		poolID: 1,
	}
	ctx := context.Background()

	if err := store.Put(ctx, "object-1", bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	r, err := store.Get(ctx, "object-1", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "ell"; got != want {
		t.Fatalf("range read: got %q, want %q", got, want)
	}

	info, err := store.Head(ctx, "object-1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Size(), int64(5); got != want {
		t.Fatalf("head size: got %d, want %d", got, want)
	}

	if err := store.Delete(ctx, "object-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "object-1"); err != nil {
		t.Fatalf("delete should be idempotent: %v", err)
	}
	if _, err := store.Head(ctx, "object-1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head after delete: got %v, want os.ErrNotExist", err)
	}
}

func TestOmniStoreMtimePersistsAcrossAdapterInstances(t *testing.T) {
	client := newFakeOmniStore()
	ctx := context.Background()
	first := &omnistoreStorage{client: client, poolID: 1, mtimes: make(map[string]time.Time)}
	if err := first.Put(ctx, "mtime-object", bytes.NewReader([]byte("value"))); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := first.Head(ctx, "mtime-object")
	if err != nil {
		t.Fatal(err)
	}

	second := &omnistoreStorage{client: client, poolID: 1, mtimes: make(map[string]time.Time)}
	secondInfo, err := second.Head(ctx, "mtime-object")
	if err != nil {
		t.Fatal(err)
	}
	if firstInfo.Mtime().IsZero() || !firstInfo.Mtime().Equal(secondInfo.Mtime()) {
		t.Fatalf("mtime was not persisted: first=%s second=%s", firstInfo.Mtime(), secondInfo.Mtime())
	}
	if err := second.Delete(ctx, "mtime-object"); err != nil {
		t.Fatal(err)
	}
}

func TestOmniStoreFragmentedPut(t *testing.T) {
	store := &omnistoreStorage{client: newFakeOmniStore(), poolID: 1}
	ctx := context.Background()
	want := make([]byte, int(omnistore.MaxChunkSize)+17)
	for i := range want {
		want[i] = byte(i % 251)
	}
	if err := store.Put(ctx, "large", bytes.NewReader(want)); err != nil {
		t.Fatalf("fragmented put: %v", err)
	}
	if info, err := store.Head(ctx, "large"); err != nil || info.Size() != int64(len(want)) {
		t.Fatalf("fragmented head: size=%d err=%v", info.Size(), err)
	}
	if got := readOmniObject(t, store, "large", int64(omnistore.MaxChunkSize)-7, 24); !bytes.Equal(got, want[omnistore.MaxChunkSize-7:omnistore.MaxChunkSize+17]) {
		t.Fatal("fragmented cross-part range does not match")
	}
	objects, more, token, err := store.List(ctx, "", "", "", "", 10, false)
	if err != nil || more || token != "" || len(objects) != 1 || objects[0].Key() != "large" || objects[0].Size() != int64(len(want)) {
		t.Fatalf("fragmented list: objects=%v more=%v token=%q err=%v", objects, more, token, err)
	}
	objects, more, token, err = store.List(ctx, "", "large", "", "", 10, false)
	if err != nil || more || token != "" || len(objects) != 0 {
		t.Fatalf("fragmented list after startAfter: objects=%v more=%v token=%q err=%v", objects, more, token, err)
	}
	if err := store.Delete(ctx, "large"); err != nil {
		t.Fatalf("fragmented delete: %v", err)
	}
	if _, err := store.Head(ctx, "large"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fragmented head after delete: %v", err)
	}
}

func TestOmniStoreList(t *testing.T) {
	store := &omnistoreStorage{client: newFakeOmniStore(), poolID: 1}
	ctx := context.Background()
	for key, value := range map[string]string{
		"a/one": "1",
		"a/two": "22",
		"b/one": "333",
	} {
		if err := store.Put(ctx, key, bytes.NewReader([]byte(value))); err != nil {
			t.Fatal(err)
		}
	}

	objects, hasMore, token, err := store.List(ctx, "", "", "", "", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore || token != "a/two" || len(objects) != 2 {
		t.Fatalf("first page: objects=%d hasMore=%v token=%q", len(objects), hasMore, token)
	}
	objects, hasMore, token, err = store.List(ctx, "", "", token, "", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore || token != "" || len(objects) != 1 || objects[0].Key() != "b/one" || objects[0].Size() != 3 {
		t.Fatalf("second page: objects=%d hasMore=%v token=%q", len(objects), hasMore, token)
	}

	objects, hasMore, _, err = store.List(ctx, "", "", "", "/", 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore || len(objects) != 2 || objects[0].Key() != "a/" || !objects[0].IsDir() || objects[1].Key() != "b/" {
		t.Fatalf("delimiter listing: objects=%v hasMore=%v", objects, hasMore)
	}

	// A raw page can contain more than one object under the same virtual
	// directory. Verify that delimiter pagination neither repeats a directory
	// nor skips the next one when the visible page limit is one.
	for key, value := range map[string]string{
		"c/one": "1",
		"c/two": "2",
		"d/one": "3",
	} {
		if err := store.Put(ctx, key, bytes.NewReader([]byte(value))); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	marker := ""
	for {
		page, more, next, err := store.List(ctx, "", "", marker, "/", 1, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 1 {
			t.Fatalf("delimiter page length: got %d, want 1", len(page))
		}
		got = append(got, page[0].Key())
		if !more {
			break
		}
		if next == "" || next == marker {
			t.Fatalf("delimiter marker did not advance: previous=%q next=%q", marker, next)
		}
		marker = next
	}
	if want := []string{"a/", "b/", "c/", "d/"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delimiter pagination: got %v, want %v", got, want)
	}
}

func readOmniObject(t *testing.T, store ObjectStorage, key string, off, limit int64) []byte {
	t.Helper()
	r, err := store.Get(context.Background(), key, off, limit)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return data
}

// TestOmniStoreAgainstCluster exercises the ObjectStorage operations currently
// backed by the OmniStore Chunk API. It deliberately does not call
// testStorage: Copy and multipart operations are still unsupported.
func TestOmniStoreAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	store, err := CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer Shutdown(store)

	ctx := context.Background()
	prefix := fmt.Sprintf("juicefs-live-%d/", time.Now().UnixNano())
	keys := []string{
		prefix + "small",
		prefix + "empty",
		prefix + "boundary",
		prefix + "large",
		prefix + "concurrent-0",
		prefix + "concurrent-1",
		prefix + "concurrent-2",
		prefix + "concurrent-3",
		prefix + "concurrent-overwrite",
	}
	for _, key := range keys {
		key := key
		t.Cleanup(func() { _ = store.Delete(ctx, key) })
	}

	// Small writes cover the final, non-4 MiB JuiceFS block and the range
	// mapping used by Get.
	smallKey := prefix + "small"
	if err := store.Put(ctx, smallKey, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("put small object: %v", err)
	}
	if got, want := string(readOmniObject(t, store, smallKey, 0, -1)), "hello"; got != want {
		t.Fatalf("full read: got %q, want %q", got, want)
	}
	if got, want := string(readOmniObject(t, store, smallKey, 1, 3)), "ell"; got != want {
		t.Fatalf("range read: got %q, want %q", got, want)
	}
	info, err := store.Head(ctx, smallKey)
	if err != nil {
		t.Fatalf("head small object: %v", err)
	}
	if info.Size() != 5 {
		t.Fatalf("head size: got %d, want 5", info.Size())
	}
	emptyKey := prefix + "empty"
	if err := store.Put(ctx, emptyKey, bytes.NewReader(nil)); err != nil {
		t.Fatalf("put empty object: %v", err)
	}
	if got := readOmniObject(t, store, emptyKey, 0, -1); len(got) != 0 {
		t.Fatalf("empty object read: got %d bytes", len(got))
	}
	if info, err := store.Head(ctx, emptyKey); err != nil || info.Size() != 0 {
		t.Fatalf("empty object head: size=%d err=%v", info.Size(), err)
	}

	// Same-key overwrite and shrink are both safe. A shrink is published via
	// an immutable generation and manifest because the Chunk API has no
	// truncate operation.
	if err := store.Put(ctx, smallKey, bytes.NewReader([]byte("hello-world"))); err != nil {
		t.Fatalf("same-key overwrite: %v", err)
	}
	if got, want := string(readOmniObject(t, store, smallKey, 0, -1)), "hello-world"; got != want {
		t.Fatalf("overwrite read: got %q, want %q", got, want)
	}
	if err := store.Put(ctx, smallKey, bytes.NewReader([]byte("bye"))); err != nil {
		t.Fatalf("shrink overwrite: %v", err)
	}
	if got, want := string(readOmniObject(t, store, smallKey, 0, -1)), "bye"; got != want {
		t.Fatalf("shrink overwrite: got %q, want %q", got, want)
	}

	// Exercise the exact 4 MiB upper bound and a range at the end of the
	// object. The oversized case is covered by the local fake-client test
	// above, so the live test focuses on the boundary accepted by the cluster.
	boundaryKey := prefix + "boundary"
	boundary := make([]byte, int(omnistore.MaxChunkSize))
	for i := range boundary {
		boundary[i] = byte(i % 251)
	}
	if err := store.Put(ctx, boundaryKey, bytes.NewReader(boundary)); err != nil {
		t.Fatalf("put max-size object: %v", err)
	}
	info, err = store.Head(ctx, boundaryKey)
	if err != nil {
		t.Fatalf("head max-size object: %v", err)
	}
	if info.Size() != int64(len(boundary)) {
		t.Fatalf("max-size head: got %d, want %d", info.Size(), len(boundary))
	}
	tail := readOmniObject(t, store, boundaryKey, int64(len(boundary)-32), 32)
	if !bytes.Equal(tail, boundary[len(boundary)-32:]) {
		t.Fatal("max-size tail range does not match written data")
	}

	// Direct ObjectStorage users may submit a logical object larger than one
	// OmniStore Chunk. Verify the adapter's manifest and multi-Chunk range path,
	// then replace it with a small object to cover representation changes.
	largeKey := prefix + "large"
	large := make([]byte, int(omnistore.MaxChunkSize)+17)
	for i := range large {
		large[i] = byte((i * 7) % 251)
	}
	if err := store.Put(ctx, largeKey, bytes.NewReader(large)); err != nil {
		t.Fatalf("put fragmented object: %v", err)
	}
	if got := readOmniObject(t, store, largeKey, int64(omnistore.MaxChunkSize)-7, 24); !bytes.Equal(got, large[omnistore.MaxChunkSize-7:]) {
		t.Fatal("fragmented cross-part range does not match written data")
	}
	if info, err := store.Head(ctx, largeKey); err != nil || info.Size() != int64(len(large)) {
		t.Fatalf("fragmented head: size=%d err=%v", info.Size(), err)
	}
	replacement := []byte("small-after-fragmented")
	if err := store.Put(ctx, largeKey, bytes.NewReader(replacement)); err != nil {
		t.Fatalf("replace fragmented object with single chunk: %v", err)
	}
	if got := readOmniObject(t, store, largeKey, 0, -1); !bytes.Equal(got, replacement) {
		t.Fatalf("replacement after fragmented object: got %q", got)
	}

	// Concurrent writes to independent Chunk IDs exercise placement, PG
	// leader forwarding and replicated commit without introducing an
	// application-level last-writer-wins assumption.
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		i := i
		key := fmt.Sprintf("%sconcurrent-%d", prefix, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("payload-%d", i))
			if err := store.Put(ctx, key, bytes.NewReader(payload)); err != nil {
				errCh <- fmt.Errorf("put %q: %w", key, err)
				return
			}
			got, err := readOmniObjectForTest(ctx, store, key)
			if err != nil {
				errCh <- fmt.Errorf("get %q: %w", key, err)
				return
			}
			if !bytes.Equal(got, payload) {
				errCh <- fmt.Errorf("read %q: got %q, want %q", key, got, payload)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}

	// Concurrent same-key writes must publish one complete payload. The
	// adapter does not promise which request wins, but a reader must never see
	// a payload assembled from both writers.
	overwriteKey := prefix + "concurrent-overwrite"
	payloadA := bytes.Repeat([]byte("A"), 64<<10)
	payloadB := bytes.Repeat([]byte("B"), 64<<10)
	errCh = make(chan error, 2)
	for _, payload := range [][]byte{payloadA, payloadB} {
		payload := payload
		go func() {
			errCh <- store.Put(ctx, overwriteKey, bytes.NewReader(payload))
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("same-key concurrent put: %v", err)
		}
	}
	got, err := readOmniObjectForTest(ctx, store, overwriteKey)
	if err != nil {
		t.Fatalf("same-key concurrent get: %v", err)
	}
	if !bytes.Equal(got, payloadA) && !bytes.Equal(got, payloadB) {
		t.Fatal("same-key concurrent get returned a mixed payload")
	}

	// Listing is served by each PG and merged by the SDK. Use a small page so
	// the test also verifies the exclusive continuation marker.
	listed := make(map[string]int64)
	marker := ""
	for {
		page, hasMore, next, err := store.List(ctx, prefix, "", marker, "", 2, false)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, object := range page {
			listed[object.Key()] = object.Size()
		}
		if !hasMore {
			break
		}
		if next == "" || next == marker {
			t.Fatalf("list marker did not advance: previous=%q next=%q", marker, next)
		}
		marker = next
	}
	if got, want := len(listed), len(keys); got != want {
		t.Fatalf("list count: got %d, want %d", got, want)
	}
	if got, want := listed[smallKey], int64(len("bye")); got != want {
		t.Fatalf("listed small size: got %d, want %d", got, want)
	}
	if got, want := listed[emptyKey], int64(0); got != want {
		t.Fatalf("listed empty size: got %d, want %d", got, want)
	}
	if got, want := listed[boundaryKey], int64(len(boundary)); got != want {
		t.Fatalf("listed boundary size: got %d, want %d", got, want)
	}
	if got, want := listed[largeKey], int64(len(replacement)); got != want {
		t.Fatalf("listed replacement size: got %d, want %d", got, want)
	}
	afterLarge, _, _, err := store.List(ctx, prefix, largeKey, "", "", 100, false)
	if err != nil {
		t.Fatalf("list after large object: %v", err)
	}
	for _, object := range afterLarge {
		if object.Key() <= largeKey || isOmniPartKey(object.Key()) {
			t.Fatalf("list after large object returned invalid entry %q", object.Key())
		}
	}

	// Exercise virtual-directory pagination against the real cluster. Two
	// objects share the first directory so a page boundary must not repeat it
	// or skip the following directory.
	delimiterPrefix := fmt.Sprintf("juicefs-live-delimiter-%d/", time.Now().UnixNano())
	delimiterKeys := []string{
		delimiterPrefix + "a/one",
		delimiterPrefix + "a/two",
		delimiterPrefix + "b/one",
	}
	for _, key := range delimiterKeys {
		key := key
		if err := store.Put(ctx, key, bytes.NewReader([]byte(key))); err != nil {
			t.Fatalf("put delimiter object %q: %v", key, err)
		}
		t.Cleanup(func() { _ = store.Delete(ctx, key) })
	}
	var directories []string
	marker = ""
	for {
		page, more, next, err := store.List(ctx, delimiterPrefix, "", marker, "/", 1, false)
		if err != nil {
			t.Fatalf("delimiter list: %v", err)
		}
		if len(page) != 1 || !page[0].IsDir() {
			t.Fatalf("delimiter page: objects=%v", page)
		}
		directories = append(directories, page[0].Key())
		if !more {
			break
		}
		if next == "" || next == marker {
			t.Fatalf("delimiter marker did not advance: previous=%q next=%q", marker, next)
		}
		marker = next
	}
	wantDirectories := []string{delimiterPrefix + "a/", delimiterPrefix + "b/"}
	if !reflect.DeepEqual(directories, wantDirectories) {
		t.Fatalf("delimiter directories: got %v, want %v", directories, wantDirectories)
	}

	// Delete is idempotent, and a deleted Chunk is no longer readable.
	if err := store.Delete(ctx, smallKey); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Delete(ctx, smallKey); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
	if _, err := store.Head(ctx, smallKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head after delete: got %v, want os.ErrNotExist", err)
	}
}

// TestOmniStoreSupportedObjectContractAgainstCluster reuses JuiceFS' common
// object contract to validate the OmniStore adapter against the real cluster.
func TestOmniStoreSupportedObjectContractAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	store, err := CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer Shutdown(store)

	testStorage(t, store)
}

func TestOmniStoreCancelledRequestsAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	store, err := CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer Shutdown(store)

	key := fmt.Sprintf("juicefs-live-cancel-%d/object", time.Now().UnixNano())
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{name: "put", call: func(ctx context.Context) error {
			return store.Put(ctx, key, bytes.NewReader([]byte("cancelled")))
		}},
		{name: "get", call: func(ctx context.Context) error {
			reader, err := store.Get(ctx, key, 0, -1)
			if reader != nil {
				_ = reader.Close()
			}
			return err
		}},
		{name: "head", call: func(ctx context.Context) error {
			_, err := store.Head(ctx, key)
			return err
		}},
		{name: "list", call: func(ctx context.Context) error {
			_, _, _, err := store.List(ctx, "juicefs-live-cancel-", "", "", "", 10, false)
			return err
		}},
		{name: "delete", call: func(ctx context.Context) error {
			return store.Delete(ctx, key)
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			started := time.Now()
			err := operation.call(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("cancelled request took %s", elapsed)
			}
		})
	}
}

func readOmniObjectForTest(ctx context.Context, store ObjectStorage, key string) ([]byte, error) {
	r, err := store.Get(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return data, nil
}
