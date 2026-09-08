/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
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

package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChunkedEncryptedConcurrentGet(t *testing.T) {
	ctx := context.Background()
	s, err := CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	dc, err := NewDataEncryptor(NewRSAEncryptor(rsaKey), AES256GCM_RSA)
	require.NoError(t, err)
	store := NewChunkedEncrypted(s, dc)

	const dataSize = 1024
	want := make([]byte, dataSize)
	for i := range want {
		want[i] = byte(i % 251)
	}
	require.NoError(t, store.Put(ctx, "key", bytes.NewReader(want)))

	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := store.Get(ctx, "key", 0, -1)
			require.NoError(t, err)
			defer r.Close()

			// Read one byte at a time to maximise the r.buf aliasing window.
			buf := make([]byte, 1)
			var got []byte
			for {
				n, readErr := r.Read(buf)
				if n > 0 {
					got = append(got, buf[:n]...)
				}
				if readErr == io.EOF {
					break
				}
				require.NoError(t, readErr)
			}
			require.Equal(t, want, got)
		}()
	}
	wg.Wait()
}

func TestChunkedEncryptedOmniStoreAgainstCluster(t *testing.T) {
	endpoint := os.Getenv("OMNISTORE_JUICEFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OMNISTORE_JUICEFS_ENDPOINT to run against a live OmniStore cluster")
	}

	base, err := CreateStorage("omnistore", endpoint, "", "", "")
	if err != nil {
		t.Fatalf("create OmniStore backend: %s", err)
	}
	defer Shutdown(base)
	key := fmt.Sprintf("juicefs-encrypted-live-%d", time.Now().UnixNano())
	dc, err := NewDataEncryptor(NewRSAEncryptor(rsaKey), AES256GCM_RSA)
	if err != nil {
		t.Fatalf("create data encryptor: %s", err)
	}
	store := NewChunkedEncrypted(base, dc)

	payload := make([]byte, plainChunkSize+97)
	for i := range payload {
		payload[i] = byte((i * 29) % 251)
	}
	ctx := context.Background()
	if err := store.Put(ctx, key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("encrypted put: %s", err)
	}
	defer func() { _ = store.Delete(ctx, key) }()

	info, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("encrypted head: err=%v", err)
	}
	if info.Size() != int64(len(payload)) {
		t.Fatalf("encrypted head: size=%d want=%d", info.Size(), len(payload))
	}
	r, err := store.Get(ctx, key, plainChunkSize-7, 32)
	if err != nil {
		t.Fatalf("encrypted range get: %s", err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatalf("encrypted range read: %s", err)
	}
	if !bytes.Equal(got, payload[plainChunkSize-7:plainChunkSize+25]) {
		t.Fatalf("encrypted range data mismatch: got %d bytes", len(got))
	}

	const readers = 16
	start := make(chan struct{})
	errCh := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := store.Get(ctx, key, 0, -1)
			if err != nil {
				errCh <- fmt.Errorf("encrypted concurrent get: %w", err)
				return
			}
			data, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil {
				errCh <- fmt.Errorf("encrypted concurrent read: %w", readErr)
				return
			}
			if closeErr != nil {
				errCh <- fmt.Errorf("encrypted concurrent close: %w", closeErr)
				return
			}
			if !bytes.Equal(data, payload) {
				errCh <- fmt.Errorf("encrypted concurrent data mismatch: got %d bytes", len(data))
				return
			}
			errCh <- nil
		}()
	}
	close(start)
	wg.Wait()
	for i := 0; i < readers; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}
