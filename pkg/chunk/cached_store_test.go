/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//nolint:errcheck
package chunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forgetSlice(store ChunkStore, sliceId uint64, size int) error {
	w := store.NewWriter(sliceId, 0)
	buf := bytes.Repeat([]byte{0x41}, size)
	if _, err := w.WriteAt(buf, 0); err != nil {
		return err
	}
	return w.Finish(size)
}

func testStore(t *testing.T, store ChunkStore) {
	writer := store.NewWriter(1, 0)
	data := []byte("hello world")
	if n, err := writer.WriteAt(data, 0); n != 11 || err != nil {
		t.Fatalf("write fail: %d %s", n, err)
	}
	offset := defaultConf.BlockSize - 3
	if n, err := writer.WriteAt(data, int64(offset)); err != nil || n != 11 {
		t.Fatalf("write fail: %d %s", n, err)
	}
	if err := writer.FlushTo(defaultConf.BlockSize + 3); err != nil {
		t.Fatalf("flush fail: %s", err)
	}
	size := offset + len(data)
	if err := writer.Finish(size); err != nil {
		t.Fatalf("finish fail: %s", err)
	}
	defer store.Remove(1, size)

	reader := store.NewReader(1, size)
	p := NewPage(make([]byte, 5))
	if n, err := reader.ReadAt(context.Background(), p, 6); n != 5 || err != nil {
		t.Fatalf("read failed: %d %s", n, err)
	} else if string(p.Data[:n]) != "world" {
		t.Fatalf("not expected: %s", string(p.Data[:n]))
	}
	p = NewPage(make([]byte, 5))
	if n, err := reader.ReadAt(context.Background(), p, 0); n != 5 || err != nil {
		t.Fatalf("read failed: %d %s", n, err)
	} else if string(p.Data[:n]) != "hello" {
		t.Fatalf("not expected: %s", string(p.Data[:n]))
	}
	p = NewPage(make([]byte, 20))
	if n, err := reader.ReadAt(context.Background(), p, offset); n != 11 || err != nil && err != io.EOF {
		t.Fatalf("read failed: %d %s", n, err)
	} else if string(p.Data[:n]) != "hello world" {
		t.Fatalf("not expected: %s", string(p.Data[:n]))
	}

	bsize := defaultConf.BlockSize / 2
	errs := make(chan error, 3)
	for i := 2; i < 5; i++ {
		go func(sliceId uint64) {
			if err := forgetSlice(store, sliceId, bsize); err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond * 100) // waiting for flush
			errs <- store.Remove(sliceId, bsize)
		}(uint64(i))
	}
	for i := 0; i < 3; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("test concurrent write failed: %s", err)
		}
	}
}

var defaultConf = Config{
	BlockSize:         1 << 20,
	CacheDir:          filepath.Join(os.TempDir(), fmt.Sprintf("diskCache-%d", os.Getpid())),
	CacheMode:         0600,
	CacheSize:         10 << 20,
	CacheChecksum:     CsNone,
	CacheScanInterval: time.Second * 300,
	MaxUpload:         1,
	MaxDownload:       200,
	MaxRetries:        10,
	PutTimeout:        time.Second,
	GetTimeout:        time.Second * 2,
	AutoCreate:        true,
	BufferSize:        10 << 20,
}

var ctx = context.Background()

func TestStoreDefault(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	_ = os.RemoveAll(defaultConf.CacheDir)
	store := NewCachedStore(mem, defaultConf, nil)
	testStore(t, store)
	if used := store.UsedMemory(); used != 0 {
		t.Fatalf("used memory %d != expect 0", used)
	}
	if cnt, used := store.(*cachedStore).bcache.stats(); cnt != 0 || used != 0 {
		t.Fatalf("cache cnt %d used %d, expect both 0", cnt, used)
	}
}

func TestOmniStoreCachedStoreAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = "memory"
	conf.Compress = "none"
	conf.BlockSize = 1 << 20
	conf.Writeback = false
	// A real three-replica backend can take longer than the short in-memory
	// test defaults while a PG leader is being selected or a delete is being
	// replicated. Keep this live contract test strict enough to catch a hang,
	// but avoid treating normal Docker scheduling jitter as a backend failure.
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	id := uint64(time.Now().UnixNano())
	length := conf.BlockSize*2 + 123
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte((i * 17) % 251)
	}

	writer := store.NewWriter(id, 0)
	if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("chunk write: n=%d err=%v", n, err)
	}
	if err := writer.Finish(length); err != nil {
		t.Fatalf("chunk finish: %v", err)
	}
	defer func() {
		if err := store.Remove(id, length); err != nil {
			t.Errorf("chunk remove: %v", err)
		}
	}()

	reader := store.NewReader(id, length)
	page := NewPage(make([]byte, length))
	defer page.Release()
	if n, err := reader.ReadAt(context.Background(), page, 0); err != nil || n != length {
		t.Fatalf("chunk full read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(page.Data, payload) {
		t.Fatal("chunk full read does not match written data")
	}

	const rangeOffset = 1<<20 - 13
	rangePage := NewPage(make([]byte, 32))
	defer rangePage.Release()
	if n, err := reader.ReadAt(context.Background(), rangePage, rangeOffset); err != nil || n != len(rangePage.Data) {
		t.Fatalf("chunk cross-block read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(rangePage.Data, payload[rangeOffset:rangeOffset+len(rangePage.Data)]) {
		t.Fatal("chunk cross-block read does not match written data")
	}
}

func TestOmniStoreCachedStoreBoundariesAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = "memory"
	conf.Compress = "none"
	conf.BlockSize = 1 << 20
	conf.Writeback = false
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	for i, length := range []int{4 << 10, conf.BlockSize, 4 << 20, 4<<20 + 1} {
		id := uint64(time.Now().UnixNano()) + uint64(i)
		payload := make([]byte, length)
		for j := range payload {
			payload[j] = byte((j*31 + i) % 251)
		}
		writer := store.NewWriter(id, 0)
		if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
			t.Fatalf("boundary %d write: n=%d err=%v", length, n, err)
		}
		if err := writer.Finish(length); err != nil {
			t.Fatalf("boundary %d finish: %v", length, err)
		}
		defer func(id uint64, length int) {
			if err := store.Remove(id, length); err != nil {
				t.Errorf("boundary %d remove: %v", length, err)
			}
		}(id, length)

		reader := store.NewReader(id, length)
		page := NewPage(make([]byte, length))
		if n, err := reader.ReadAt(context.Background(), page, 0); err != nil || n != length {
			page.Release()
			t.Fatalf("boundary %d full read: n=%d err=%v", length, n, err)
		}
		if !bytes.Equal(page.Data, payload) {
			page.Release()
			t.Fatalf("boundary %d full read does not match written data", length)
		}
		page.Release()

		const rangeLength = 32
		rangeOffset := length - rangeLength
		if rangeOffset < 0 {
			rangeOffset = 0
		}
		rangePage := NewPage(make([]byte, rangeLength))
		if n, err := reader.ReadAt(context.Background(), rangePage, rangeOffset); err != nil || n != rangeLength {
			rangePage.Release()
			t.Fatalf("boundary %d tail read: n=%d err=%v", length, n, err)
		}
		if !bytes.Equal(rangePage.Data, payload[rangeOffset:rangeOffset+rangeLength]) {
			rangePage.Release()
			t.Fatalf("boundary %d tail read does not match written data", length)
		}
		rangePage.Release()
	}
}

func TestOmniStoreCachedStoreConcurrentReadAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = "memory"
	conf.Compress = "none"
	conf.BlockSize = 1 << 20
	conf.Writeback = false
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	id := uint64(time.Now().UnixNano())
	length := conf.BlockSize
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte((i * 61) % 251)
	}
	writer := store.NewWriter(id, 0)
	if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("concurrent read write: n=%d err=%v", n, err)
	}
	if err := writer.Finish(length); err != nil {
		t.Fatalf("concurrent read finish: %v", err)
	}
	defer func() {
		if err := store.Remove(id, length); err != nil {
			t.Errorf("concurrent read remove: %v", err)
		}
	}()
	if err := store.EvictCache(id, uint32(length), nil); err != nil {
		t.Fatalf("evict before concurrent read: %v", err)
	}

	const readers = 16
	const readOffset = 123
	const readLength = 64 << 10
	start := make(chan struct{})
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			<-start
			reader := store.NewReader(id, length)
			page := NewPage(make([]byte, readLength))
			defer page.Release()
			n, err := reader.ReadAt(context.Background(), page, readOffset)
			if err != nil {
				errCh <- err
				return
			}
			if n != readLength {
				errCh <- fmt.Errorf("short concurrent read: got %d, want %d", n, readLength)
				return
			}
			if !bytes.Equal(page.Data, payload[readOffset:readOffset+readLength]) {
				errCh <- errors.New("concurrent read does not match written data")
				return
			}
			errCh <- nil
		}()
	}
	close(start)
	for i := 0; i < readers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent read: %v", err)
		}
	}
}

func TestOmniStoreCachedStorePrefetchAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = "memory"
	conf.Compress = "none"
	conf.BlockSize = 1 << 20
	conf.Prefetch = 1
	conf.Writeback = false
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	id := uint64(time.Now().UnixNano())
	length := conf.BlockSize
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte((i * 47) % 251)
	}
	writer := store.NewWriter(id, 0)
	if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("prefetch write: n=%d err=%v", n, err)
	}
	if err := writer.Finish(length); err != nil {
		t.Fatalf("prefetch finish: %v", err)
	}
	defer func() {
		if err := store.Remove(id, length); err != nil {
			t.Errorf("prefetch remove: %v", err)
		}
	}()
	if err := store.EvictCache(id, uint32(length), nil); err != nil {
		t.Fatalf("evict before prefetch: %v", err)
	}

	reader := store.NewReader(id, length)
	const rangeOffset = 123
	rangePage := NewPage(make([]byte, 32))
	if n, err := reader.ReadAt(context.Background(), rangePage, rangeOffset); err != nil || n != len(rangePage.Data) {
		rangePage.Release()
		t.Fatalf("prefetch range read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(rangePage.Data, payload[rangeOffset:rangeOffset+len(rangePage.Data)]) {
		rangePage.Release()
		t.Fatal("prefetch range read does not match written data")
	}
	rangePage.Release()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cached := false
		if err := store.CheckCache(id, uint32(length), nil, func(exists bool, _ string, _ int) {
			cached = cached || exists
		}); err != nil {
			t.Fatalf("check prefetched cache: %v", err)
		}
		if cached {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("prefetch did not populate the cache from OmniStore")
}

func TestOmniStoreCachedStoreWritebackAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = t.TempDir()
	conf.CacheSize = 64 << 20
	conf.Compress = "none"
	conf.BlockSize = 1 << 20
	conf.Writeback = true
	conf.WritebackThresholdSize = conf.BlockSize + 1
	conf.UploadDelay = 0
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	id := uint64(time.Now().UnixNano())
	length := 64 << 10
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte((i * 53) % 251)
	}
	writer := store.NewWriter(id, 0)
	if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("writeback write: n=%d err=%v", n, err)
	}
	if err := writer.Finish(length); err != nil {
		t.Fatalf("writeback finish: %v", err)
	}
	defer func() {
		if err := store.Remove(id, length); err != nil {
			t.Errorf("writeback remove: %v", err)
		}
	}()

	rSlice := sliceForRead(id, length, store.(*cachedStore))
	key := rSlice.key(0)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := blob.Head(context.Background(), key); err == nil {
			reader := store.NewReader(id, length)
			if err := store.EvictCache(id, uint32(length), nil); err != nil {
				t.Fatalf("evict writeback cache: %v", err)
			}
			page := NewPage(make([]byte, length))
			if n, err := reader.ReadAt(context.Background(), page, 0); err != nil || n != length {
				page.Release()
				t.Fatalf("read written-back data: n=%d err=%v", n, err)
			}
			if !bytes.Equal(page.Data, payload) {
				page.Release()
				t.Fatal("written-back data does not match")
			}
			page.Release()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("writeback object %q did not reach OmniStore", key)
}

func TestOmniStoreCachedStoreCompressedAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}
	blob, err := object.CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatalf("create OmniStore backend: %s", err)
	}
	defer object.Shutdown(blob)

	conf := defaultConf
	conf.CacheDir = "memory"
	conf.Compress = "lz4"
	conf.BlockSize = 1 << 20
	conf.Writeback = false
	conf.PutTimeout = 10 * time.Second
	conf.GetTimeout = 10 * time.Second
	store := NewCachedStore(blob, conf, nil)

	id := uint64(time.Now().UnixNano())
	length := conf.BlockSize + 123
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte((i / 4096) % 7)
	}
	writer := store.NewWriter(id, 0)
	if n, err := writer.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("compressed chunk write: n=%d err=%v", n, err)
	}
	if err := writer.Finish(length); err != nil {
		t.Fatalf("compressed chunk finish: %v", err)
	}
	defer func() {
		if err := store.Remove(id, length); err != nil {
			t.Errorf("compressed chunk remove: %v", err)
		}
	}()

	reader := store.NewReader(id, length)
	page := NewPage(make([]byte, length))
	defer page.Release()
	if n, err := reader.ReadAt(context.Background(), page, 0); err != nil || n != length {
		t.Fatalf("compressed chunk read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(page.Data, payload) {
		t.Fatal("compressed chunk read does not match written data")
	}
}

func TestStoreMemCache(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheDir = "memory"
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
	if used := store.UsedMemory(); used != 0 {
		t.Fatalf("used memory %d != expect 0", used)
	}
	if cnt, used := store.(*cachedStore).bcache.stats(); cnt != 0 || used != 0 {
		t.Fatalf("cache cnt %d used %d, expect both 0", cnt, used)
	}
}
func TestStoreCompressed(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.Compress = "lz4"
	conf.AutoCreate = false
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
}

func TestStoreLimited(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.UploadLimit = 1e6
	conf.DownloadLimit = 1e6
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
}

func TestStoreFull(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.FreeSpace = 0.9999
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
}

func TestStoreSmallBuffer(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.BufferSize = 1 << 20
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
}

func TestStoreAsync(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.Writeback = true
	p := filepath.Join(conf.CacheDir, stagingDir, "chunks/0/0/123_0_4")
	os.MkdirAll(filepath.Dir(p), 0744)
	f, _ := os.Create(p)
	f.WriteString("good")
	f.Close()
	store := NewCachedStore(mem, conf, nil)
	time.Sleep(time.Millisecond * 50) // wait for scan to finish
	in, err := mem.Get(ctx, "chunks/0/0/123_0_4", 0, -1)
	if err != nil {
		t.Fatalf("staging object should be upload")
	}
	data, _ := io.ReadAll(in)
	if string(data) != "good" {
		t.Fatalf("data %s != expect good", data)
	}
	testStore(t, store)
}

func TestForceUpload(t *testing.T) {
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	config := defaultConf
	_ = os.RemoveAll(config.CacheDir)
	config.Writeback = true
	config.WritebackThresholdSize = config.BlockSize + 1
	config.UploadDelay = time.Hour
	config.BlockSize = 4 << 20
	store := NewCachedStore(blob, config, nil)
	cleanCache := func() {
		rSlice := sliceForRead(1, 1024, store.(*cachedStore))
		for _, i := range rSlice.blockIndexes(nil) {
			store.(*cachedStore).bcache.remove(rSlice.key(i), true)
		}
	}
	readSlice := func(id uint64, length int) error {
		p := NewPage(make([]byte, length))
		r := store.NewReader(id, length)
		_, err := r.ReadAt(context.Background(), p, 0)
		return err
	}

	// write to cache
	w := store.NewWriter(1, 0)
	if _, err := w.WriteAt(make([]byte, 1024), 0); err != nil {
		t.Fatalf("write fail: %s", err)
	}
	if err := w.Finish(1024); err != nil {
		t.Fatalf("write fail: %s", err)
	}
	cleanCache()
	if readSlice(1, 1024) == nil {
		t.Fatalf("read slice 1 should fail")
	}

	// write to os
	w = store.NewWriter(2, 0)
	w.SetWriteback(false)
	if _, err := w.WriteAt(make([]byte, 1024), 0); err != nil {
		t.Fatalf("write fail: %s", err)
	}
	if err := w.Finish(1024); err != nil {
		t.Fatalf("write fail: %s", err)
	}
	cleanCache()
	if readSlice(2, 1024) != nil {
		t.Fatalf("check slice 2 should success")
	}
}

func TestStoreDelayed(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.Writeback = true
	conf.UploadDelay = time.Millisecond * 200
	store := NewCachedStore(mem, conf, nil)
	time.Sleep(time.Second) // waiting for cache scanned
	testStore(t, store)
	if err := forgetSlice(store, 10, 1024); err != nil {
		t.Fatalf("forge slice 10 1024: %s", err)
	}
	defer store.Remove(10, 1024)
	time.Sleep(time.Second) // waiting for upload
	if _, err := mem.Head(ctx, "chunks/0/0/10_0_1024"); err != nil {
		t.Fatalf("head object 10_0_1024: %s", err)
	}
}

func TestStoreMultiBuckets(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.HashPrefix = true
	store := NewCachedStore(mem, conf, nil)
	testStore(t, store)
}

func TestFillCache(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheSize = 10 << 20
	conf.FreeSpace = 0.01
	_ = os.RemoveAll(conf.CacheDir)
	store := NewCachedStore(mem, conf, nil)
	if err := forgetSlice(store, 10, 1024); err != nil {
		t.Fatalf("forge slice 10 1024: %s", err)
	}
	defer store.Remove(10, 1024)
	bsize := conf.BlockSize
	if err := forgetSlice(store, 11, bsize); err != nil {
		t.Fatalf("forge slice 11 %d: %s", bsize, err)
	}
	defer store.Remove(11, bsize)

	time.Sleep(time.Millisecond * 100) // waiting for flush
	bcache := store.(*cachedStore).bcache
	if cnt, used := bcache.stats(); cnt != 1 || used != 1024+4096 { // only chunk 10 cached
		t.Fatalf("cache cnt %d used %d, expect cnt 1 used 5120", cnt, used)
	}
	if err := store.FillCache(10, 1024, nil); err != nil {
		t.Fatalf("fill cache 10 1024: %s", err)
	}
	if err := store.FillCache(11, uint32(bsize), nil); err != nil {
		t.Fatalf("fill cache 11 %d: %s", bsize, err)
	}
	time.Sleep(time.Second)
	expect := int64(1024 + 4096 + bsize + 4096)
	if cnt, used := bcache.stats(); cnt != 2 || used != expect {
		t.Fatalf("cache cnt %d used %d, expect cnt 2 used %d", cnt, used, expect)
	}

	var missBytes uint64
	handler := func(exists bool, loc string, size int) {
		if !exists {
			missBytes += uint64(size)
		}
	}
	// check
	err := store.CheckCache(10, 1024, nil, handler)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), missBytes)

	missBytes = 0
	err = store.CheckCache(11, uint32(bsize), nil, handler)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), missBytes)

	// evict slice 11
	err = store.EvictCache(11, uint32(bsize), nil)
	assert.Nil(t, err)

	// stat
	if cnt, used := bcache.stats(); cnt != 1 || used != 1024+4096 { // only chunk 10 cached
		t.Fatalf("cache cnt %d used %d, expect cnt 1 used 5120", cnt, used)
	}

	// check again
	missBytes = 0
	err = store.CheckCache(11, uint32(bsize), nil, handler)
	assert.Nil(t, err)
	assert.Equal(t, uint64(bsize), missBytes)
}

func BenchmarkCachedRead(b *testing.B) {
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	config := defaultConf
	config.BlockSize = 4 << 20
	store := NewCachedStore(blob, config, nil)
	w := store.NewWriter(1, 0)
	if _, err := w.WriteAt(make([]byte, 1024), 0); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	if err := w.Finish(1024); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	time.Sleep(time.Millisecond * 100)
	p := NewPage(make([]byte, 1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := store.NewReader(1, 1024)
		if n, err := r.ReadAt(context.Background(), p, 0); err != nil || n != 1024 {
			b.FailNow()
		}
	}
}

func BenchmarkUncachedRead(b *testing.B) {
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	config := defaultConf
	config.BlockSize = 4 << 20
	config.CacheSize = 0
	store := NewCachedStore(blob, config, nil)
	w := store.NewWriter(2, 0)
	if _, err := w.WriteAt(make([]byte, 1024), 0); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	if err := w.Finish(1024); err != nil {
		b.Fatalf("write fail: %s", err)
	}
	p := NewPage(make([]byte, 1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := store.NewReader(2, 1024)
		if n, err := r.ReadAt(context.Background(), p, 0); err != nil || n != 1024 {
			b.FailNow()
		}
	}
}

type dStore struct {
	object.ObjectStorage
	cnt int32
}

func (s *dStore) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	atomic.AddInt32(&s.cnt, 1)
	return nil, errors.New("not found")
}

func TestStoreRetry(t *testing.T) {
	s := &dStore{}
	cs := NewCachedStore(s, defaultConf, nil)
	p := NewPage(nil)
	defer p.Release()
	cs.(*cachedStore).load(context.TODO(), "non", p, false, false) // wont retry
	require.Equal(t, int32(1), s.cnt)
}

func TestBlockIndexes(t *testing.T) {
	conf := defaultConf
	conf.BlockSize = 1 << 20 // 1MiB
	store := &cachedStore{conf: conf}
	const size = 4 << 20 // 4 blocks

	cases := []struct {
		name   string
		length int
		parts  []Range
		expect []int
	}{
		{"nil parts select every block", size, nil, []int{0, 1, 2, 3}},
		{"one part inside a block", size, []Range{{Off: 10, Len: 10}}, []int{0}},
		{"part spanning two blocks", size, []Range{{Off: (1 << 20) - 1, Len: 2}}, []int{0, 1}},
		{"last block only", size, []Range{{Off: 3 << 20, Len: 5}}, []int{3}},
		// two parts in the same block must not process it twice
		{"same block once", size, []Range{{Off: 0, Len: 10}, {Off: 1000, Len: 10}}, []int{0}},
		{"adjacent parts sharing a block", size, []Range{{Off: 0, Len: 1 << 20}, {Off: 1 << 20, Len: 10}}, []int{0, 1}},
		{"part beyond the object is clipped", size, []Range{{Off: 3 << 20, Len: 100 << 20}}, []int{3}},
		{"part starting past the end", size, []Range{{Off: 8 << 20, Len: 10}}, nil},
		{"empty part", size, []Range{{Off: 0, Len: 0}}, nil},
		{"zero length object", 0, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := sliceForRead(1, c.length, store)
			if got := r.blockIndexes(c.parts); !reflect.DeepEqual(got, c.expect) {
				t.Fatalf("blockIndexes(%v) = %v, want %v", c.parts, got, c.expect)
			}
		})
	}
}
