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

package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	omnistore "omnistorage/sdk/go"
)

// omniChunkClient is kept deliberately smaller than omnistore.Client so the
// ObjectStorage adapter can be unit-tested without starting a Monitor/OSD
// cluster. omnistore.Client satisfies this interface.
type omniChunkClient interface {
	omnistore.ChunkStore
	Close() error
}

// omnistoreStorage adapts JuiceFS object-block operations to OmniStore's
// fixed-size Chunk API. The normal JuiceFS Chunk path already splits files
// into objects, so those objects use one OmniStore Chunk. Direct users of the
// ObjectStorage interface (for example sync) may submit a larger logical
// object; those objects are represented by immutable data Chunks plus a small
// manifest Chunk.
//
// The adapter implements the object operations used by JuiceFS' ordinary
// object and maintenance paths. Multipart parts are stored as temporary,
// hidden OmniStore Chunks and are published as a normal object only by
// CompleteUpload.
type omnistoreStorage struct {
	DefaultObjectStorage
	client   omniChunkClient
	poolID   uint64
	prefix   string
	endpoint string
	stateMu  sync.RWMutex
	uploads  map[string]*omniMultipartUpload
	// The current OmniStore Chunk metadata contains size but not mtime. Keep
	// the timestamp assigned by this adapter so List and Head expose a stable,
	// recent value during the lifetime of a JuiceFS process. Objects discovered
	// after process start get an observation-time value as a best effort.
	mtimes map[string]time.Time
}

var _ ObjectStorage = (*omnistoreStorage)(nil)

const delimiterListTokenPrefix = "omnistore-delimiter-v1:"

// OmniStore does not retain a zero-length Chunk. Keep an empty logical
// object in a separate reserved Chunk so JuiceFS' object contract can still
// represent empty files and directory markers without changing the Chunk API.
const emptyObjectSuffix = ".__omnistore_empty_v1__"

const (
	manifestObjectSuffix = ".__omnistore_manifest_v1__"
	partObjectSuffix     = ".__omnistore_part_v1__"
	mtimeObjectSuffix    = ".__omnistore_mtime_v1__"
	manifestEncodedSize  = 32
)

type omniMultipartUpload struct {
	mu      sync.Mutex
	key     string
	created time.Time
	parts   map[int]omniMultipartPart
}

type omniMultipartPart struct {
	ref  omnistore.ChunkRef
	part Part
}

type omniMultipartReader struct {
	ctx    context.Context
	client omniChunkClient
	parts  []omniMultipartPart
	index  int
	offset uint32
}

func (r *omniMultipartReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	total := 0
	for total < len(p) && r.index < len(r.parts) {
		part := r.parts[r.index]
		if r.offset >= uint32(part.part.Size) {
			r.index++
			r.offset = 0
			continue
		}

		remaining := part.part.Size - int(r.offset)
		want := len(p) - total
		if want > remaining {
			want = remaining
		}
		n, err := r.client.ReadAt(r.ctx, part.ref, r.offset, p[total:total+want])
		if n > 0 {
			total += n
			r.offset += uint32(n)
		}
		if n != want {
			if err != nil {
				return total, err
			}
			return total, io.ErrUnexpectedEOF
		}
		// A client may return io.EOF together with the final requested bytes.
		// The part boundary, rather than that transient status, determines
		// whether the multipart stream is complete.
	}
	if total > 0 {
		return total, nil
	}
	return 0, io.EOF
}

var manifestMagic = [8]byte{'O', 'M', 'N', 'I', 'M', 'F', 'S', 1}
var omniGeneration uint64

type omniManifest struct {
	generation uint64
	size       uint64
	partCount  uint64
}

func encodeOmniManifest(manifest omniManifest) []byte {
	data := make([]byte, manifestEncodedSize)
	copy(data[:len(manifestMagic)], manifestMagic[:])
	binary.BigEndian.PutUint64(data[8:16], manifest.generation)
	binary.BigEndian.PutUint64(data[16:24], manifest.size)
	binary.BigEndian.PutUint64(data[24:32], manifest.partCount)
	return data
}

func decodeOmniManifest(data []byte) (omniManifest, error) {
	if len(data) < manifestEncodedSize || !bytes.Equal(data[:len(manifestMagic)], manifestMagic[:]) {
		return omniManifest{}, fmt.Errorf("invalid omnistore manifest")
	}
	manifest := omniManifest{
		generation: binary.BigEndian.Uint64(data[8:16]),
		size:       binary.BigEndian.Uint64(data[16:24]),
		partCount:  binary.BigEndian.Uint64(data[24:32]),
	}
	if manifest.generation == 0 || manifest.size == 0 || manifest.partCount == 0 {
		return omniManifest{}, fmt.Errorf("invalid omnistore manifest values")
	}
	wantParts := (manifest.size + uint64(omnistore.MaxChunkSize) - 1) / uint64(omnistore.MaxChunkSize)
	if manifest.partCount != wantParts {
		return omniManifest{}, fmt.Errorf("invalid omnistore manifest part count")
	}
	return manifest, nil
}

func nextOmniGeneration() uint64 {
	sequence := atomic.AddUint64(&omniGeneration, 1)
	generation := uint64(time.Now().UnixNano()) ^ (sequence << 1)
	if generation == 0 {
		generation = sequence
	}
	return generation
}

func encodeDelimiterListToken(marker, visible string) string {
	encode := func(value string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(value))
	}
	return delimiterListTokenPrefix + encode(marker) + "." + encode(visible)
}

func decodeDelimiterListToken(token string) (marker, visible string, ok bool) {
	if !strings.HasPrefix(token, delimiterListTokenPrefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(token, delimiterListTokenPrefix), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	decode := func(value string) (string, error) {
		data, err := base64.RawURLEncoding.DecodeString(value)
		return string(data), err
	}
	marker, err := decode(parts[0])
	if err != nil {
		return "", "", false
	}
	visible, err = decode(parts[1])
	if err != nil {
		return "", "", false
	}
	return marker, visible, true
}

func (s *omnistoreStorage) String() string {
	return s.endpoint
}

func (s *omnistoreStorage) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              1,
		MaxPartSize:              int64(omnistore.MaxChunkSize),
		MaxPartCount:             10000,
	}
}

// Create is intentionally a no-op. OmniStore pools are provisioned by the
// control plane and the object adapter must not create or mutate cluster
// topology during JuiceFS initialization.
func (s *omnistoreStorage) Create(context.Context) error {
	return nil
}

func (s *omnistoreStorage) ref(key string) omnistore.ChunkRef {
	return omnistore.ChunkRef{PoolID: s.poolID, ChunkID: s.prefix + key}
}

func (s *omnistoreStorage) rememberMtime(ctx context.Context, key string) error {
	mtime := time.Now()
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(mtime.UnixNano()))
	n, err := s.client.WriteAt(ctx, s.mtimeRef(key), 0, data)
	if err != nil {
		return mapOmniError(err)
	}
	if n != len(data) {
		return fmt.Errorf("omnistore put %q: short mtime write: got %d, want %d", key, n, len(data))
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.mtimes == nil {
		s.mtimes = make(map[string]time.Time)
	}
	s.mtimes[key] = mtime
	return nil
}

func (s *omnistoreStorage) mtimeFor(ctx context.Context, key string) (time.Time, error) {
	s.stateMu.RLock()
	if mtime, ok := s.mtimes[key]; ok {
		s.stateMu.RUnlock()
		return mtime, nil
	}
	s.stateMu.RUnlock()
	if info, err := s.client.Stat(ctx, s.mtimeRef(key)); err == nil {
		if info.Size != 8 {
			return time.Time{}, fmt.Errorf("omnistore mtime %q: invalid size %d", key, info.Size)
		}
		data := make([]byte, 8)
		n, readErr := s.client.ReadAt(ctx, s.mtimeRef(key), 0, data)
		if readErr != nil {
			return time.Time{}, mapOmniError(readErr)
		}
		if n != len(data) {
			return time.Time{}, fmt.Errorf("omnistore mtime %q: short read: got %d, want %d", key, n, len(data))
		}
		mtime := time.Unix(0, int64(binary.BigEndian.Uint64(data)))
		s.stateMu.Lock()
		if s.mtimes == nil {
			s.mtimes = make(map[string]time.Time)
		}
		s.mtimes[key] = mtime
		s.stateMu.Unlock()
		return mtime, nil
	} else if !errors.Is(err, omnistore.ErrNotFound) {
		return time.Time{}, mapOmniError(err)
	}
	mtime := time.Now()
	s.stateMu.Lock()
	if s.mtimes == nil {
		s.mtimes = make(map[string]time.Time)
	}
	s.mtimes[key] = mtime
	s.stateMu.Unlock()
	return mtime, nil
}

func (s *omnistoreStorage) emptyRef(key string) omnistore.ChunkRef {
	return s.ref(key + emptyObjectSuffix)
}

func (s *omnistoreStorage) manifestRef(key string) omnistore.ChunkRef {
	return s.ref(key + manifestObjectSuffix)
}

func (s *omnistoreStorage) mtimeRef(key string) omnistore.ChunkRef {
	return s.ref(key + mtimeObjectSuffix)
}

// JuiceFS chunk keys are internal block objects. Their size and lifetime are
// carried by the chunk metadata, so persisting a separate mtime Chunk for
// every block only adds another replicated write without changing JuiceFS'
// observable semantics. Keep the richer mtime representation for ordinary
// ObjectStorage users, where Head/List may expose it to callers.
func isJuiceFSChunkKey(key string) bool {
	return strings.HasPrefix(key, "chunks/") || strings.Contains(key, "/chunks/")
}

func (s *omnistoreStorage) partRef(key string, generation, part uint64) omnistore.ChunkRef {
	return s.ref(key + partObjectSuffix + strconv.FormatUint(generation, 36) + "/" + strconv.FormatUint(part, 36))
}

func (s *omnistoreStorage) multipartPartRef(key, uploadID string, num int, etag string) omnistore.ChunkRef {
	return s.ref(key + partObjectSuffix + "mp/" + uploadID + "/" + strconv.Itoa(num) + "/" + etag)
}

func (s *omnistoreStorage) loadManifest(ctx context.Context, key string) (*omniManifest, error) {
	info, err := s.client.Stat(ctx, s.manifestRef(key))
	if errors.Is(err, omnistore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Size < manifestEncodedSize {
		return nil, fmt.Errorf("omnistore manifest %q is truncated", key)
	}
	data := make([]byte, manifestEncodedSize)
	n, err := s.client.ReadAt(ctx, s.manifestRef(key), 0, data)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, fmt.Errorf("omnistore manifest %q: short read: got %d, want %d", key, n, len(data))
	}
	manifest, err := decodeOmniManifest(data)
	if err != nil {
		return nil, fmt.Errorf("omnistore manifest %q: %w", key, err)
	}
	return &manifest, nil
}

func isOmniPartKey(key string) bool {
	index := strings.LastIndex(key, partObjectSuffix)
	if index < 0 {
		return false
	}
	rest := key[index+len(partObjectSuffix):]
	slash := strings.IndexByte(rest, '/')
	return slash > 0 && slash < len(rest)-1
}

func (s *omnistoreStorage) logicalKey(key string) (string, bool) {
	if !strings.HasPrefix(key, s.prefix) {
		return "", false
	}
	return strings.TrimPrefix(key, s.prefix), true
}

func (s *omnistoreStorage) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	return s.getLocked(ctx, key, off, limit, getters...)
}

func (s *omnistoreStorage) getLocked(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	if off < 0 || limit < -1 {
		return nil, fmt.Errorf("omnistore get %q: %w", key, omnistore.ErrInvalidArgument)
	}

	manifest, err := s.loadManifest(ctx, key)
	if err != nil {
		return nil, mapOmniError(err)
	}
	if manifest != nil {
		return s.getFragmented(ctx, key, *manifest, off, limit, getters...)
	}

	if _, err := s.client.Stat(ctx, s.emptyRef(key)); err == nil {
		attrs := ApplyGetters(getters...)
		if attrs.requestSize != nil {
			*attrs.requestSize = 0
		}
		return io.NopCloser(bytes.NewReader(nil)), nil
	} else if !errors.Is(err, omnistore.ErrNotFound) {
		return nil, mapOmniError(err)
	}

	info, err := s.client.Stat(ctx, s.ref(key))
	if err != nil {
		return nil, mapOmniError(err)
	}

	size := int64(info.Size)
	if off >= size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := size
	if limit > 0 && off+limit < end {
		end = off + limit
	}
	length := end - off
	data := make([]byte, length)
	n, err := s.client.ReadAt(ctx, s.ref(key), uint32(off), data)
	if err != nil {
		return nil, mapOmniError(err)
	}
	if n != len(data) {
		return nil, fmt.Errorf("omnistore get %q: %w: got %d, want %d", key, io.ErrUnexpectedEOF, n, len(data))
	}
	attrs := ApplyGetters(getters...)
	if attrs.requestSize != nil {
		*attrs.requestSize = int64(n)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *omnistoreStorage) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	return s.putReaderLocked(ctx, key, in, getters...)
}

func (s *omnistoreStorage) putReaderLocked(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	if in == nil {
		return fmt.Errorf("omnistore put %q: %w", key, omnistore.ErrInvalidArgument)
	}

	// Read at most one byte beyond the Chunk limit. Keeping the common path
	// bounded preserves the one-object/one-Chunk layout used by JuiceFS' normal
	// block path, while the extra byte detects direct large-object uploads.
	data, err := io.ReadAll(io.LimitReader(in, int64(omnistore.MaxChunkSize)+1))
	if err != nil {
		return err
	}
	if len(data) > int(omnistore.MaxChunkSize) {
		err = s.putFragmented(ctx, key, data[:omnistore.MaxChunkSize], io.MultiReader(bytes.NewReader(data[omnistore.MaxChunkSize:]), in), getters...)
	} else {
		err = s.putSingle(ctx, key, data, getters...)
	}
	if err == nil && !isJuiceFSChunkKey(key) {
		if mtimeErr := s.rememberMtime(ctx, key); mtimeErr != nil {
			return mtimeErr
		}
	}
	return err
}

func (s *omnistoreStorage) Copy(ctx context.Context, dst, src string) error {
	r, err := s.getLocked(ctx, src, 0, -1)
	if err != nil {
		return err
	}
	err = s.putReaderLocked(ctx, dst, r)
	closeErr := r.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *omnistoreStorage) CreateMultipartUpload(_ context.Context, key string) (*MultipartUpload, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.uploads == nil {
		s.uploads = make(map[string]*omniMultipartUpload)
	}
	uploadID := fmt.Sprintf("omnistore-multipart-%x", nextOmniGeneration())
	s.uploads[uploadID] = &omniMultipartUpload{
		key:     key,
		created: time.Now(),
		parts:   make(map[int]omniMultipartPart),
	}
	return &MultipartUpload{
		MinPartSize: 1,
		MaxCount:    10000,
		UploadID:    uploadID,
	}, nil
}

func (s *omnistoreStorage) uploadPartLocked(ctx context.Context, key, uploadID string, num int, body []byte) (*Part, error) {
	if num <= 0 || num > 10000 {
		return nil, fmt.Errorf("omnistore upload part %d: %w", num, omnistore.ErrInvalidArgument)
	}
	if len(body) == 0 || len(body) > int(omnistore.MaxChunkSize) {
		return nil, fmt.Errorf("omnistore upload part %d: size %d is outside [1, %d]", num, len(body), omnistore.MaxChunkSize)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	upload, ok := s.uploads[uploadID]
	s.stateMu.RUnlock()
	if !ok || upload.key != key {
		return nil, fmt.Errorf("omnistore upload %q: %w", uploadID, os.ErrNotExist)
	}
	upload.mu.Lock()
	defer upload.mu.Unlock()
	checksum := sha256.Sum256(body)
	etag := hex.EncodeToString(checksum[:])
	ref := s.multipartPartRef(key, uploadID, num, etag)
	n, err := s.client.WriteAt(ctx, ref, 0, body)
	if err != nil {
		return nil, mapOmniError(err)
	}
	if n != len(body) {
		return nil, fmt.Errorf("omnistore upload part %d: short write: got %d, want %d", num, n, len(body))
	}
	part := Part{Num: num, Size: len(body), ETag: etag}
	if old, exists := upload.parts[num]; exists && old.ref.ChunkID != ref.ChunkID {
		// The new content is published in a new immutable Chunk. A failed
		// cleanup only leaves an unreachable temporary part, which Delete can
		// reclaim by listing the upload prefix.
		_ = s.deleteRef(ctx, old.ref)
	}
	upload.parts[num] = omniMultipartPart{ref: ref, part: part}
	return &part, nil
}

func (s *omnistoreStorage) UploadPart(ctx context.Context, key, uploadID string, num int, body []byte) (*Part, error) {
	return s.uploadPartLocked(ctx, key, uploadID, num, body)
}

func (s *omnistoreStorage) UploadPartCopy(ctx context.Context, key, uploadID string, num int, srcKey string, off, size int64) (*Part, error) {
	if off < 0 || size <= 0 || size > int64(omnistore.MaxChunkSize) {
		return nil, fmt.Errorf("omnistore upload part copy: invalid range off=%d size=%d", off, size)
	}
	r, err := s.getLocked(ctx, srcKey, off, size)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return s.uploadPartLocked(ctx, key, uploadID, num, body)
}

func (s *omnistoreStorage) CompleteUpload(ctx context.Context, key, uploadID string, parts []*Part) error {
	s.stateMu.RLock()
	upload, ok := s.uploads[uploadID]
	s.stateMu.RUnlock()
	if !ok || upload.key != key {
		return fmt.Errorf("omnistore upload %q: %w", uploadID, os.ErrNotExist)
	}
	upload.mu.Lock()
	defer upload.mu.Unlock()
	ordered := make([]omniMultipartPart, 0, len(parts))
	lastNum := 0
	for _, requested := range parts {
		if requested == nil || requested.Num <= lastNum {
			return fmt.Errorf("omnistore upload %q: parts are not strictly ordered", uploadID)
		}
		stored, exists := upload.parts[requested.Num]
		if !exists || requested.Size != stored.part.Size || requested.ETag != stored.part.ETag {
			return fmt.Errorf("omnistore upload %q: part %d does not match uploaded content", uploadID, requested.Num)
		}
		ordered = append(ordered, stored)
		lastNum = requested.Num
	}

	reader := &omniMultipartReader{ctx: ctx, client: s.client, parts: ordered}
	if err := s.putReaderLocked(ctx, key, reader); err != nil {
		return err
	}
	for _, stored := range upload.parts {
		if err := s.deleteRef(ctx, stored.ref); err != nil {
			return mapOmniError(err)
		}
	}
	s.stateMu.Lock()
	delete(s.uploads, uploadID)
	s.stateMu.Unlock()
	return nil
}

func (s *omnistoreStorage) AbortUpload(ctx context.Context, key, uploadID string) {
	s.stateMu.RLock()
	upload, ok := s.uploads[uploadID]
	s.stateMu.RUnlock()
	if !ok || upload.key != key {
		return
	}
	upload.mu.Lock()
	defer upload.mu.Unlock()
	for _, part := range upload.parts {
		_ = s.deleteRef(ctx, part.ref)
	}
	s.stateMu.Lock()
	delete(s.uploads, uploadID)
	s.stateMu.Unlock()
}

func (s *omnistoreStorage) ListUploads(_ context.Context, marker string) ([]*PendingPart, string, error) {
	s.stateMu.RLock()
	if len(s.uploads) == 0 {
		s.stateMu.RUnlock()
		return nil, "", nil
	}
	ids := make([]string, 0, len(s.uploads))
	for id := range s.uploads {
		if marker == "" || id > marker {
			ids = append(ids, id)
		}
	}
	s.stateMu.RUnlock()
	sort.Strings(ids)
	result := make([]*PendingPart, 0, len(ids))
	for _, id := range ids {
		s.stateMu.RLock()
		upload := s.uploads[id]
		s.stateMu.RUnlock()
		if upload == nil {
			continue
		}
		upload.mu.Lock()
		result = append(result, &PendingPart{Key: upload.key, UploadID: id, Created: upload.created})
		upload.mu.Unlock()
	}
	return result, "", nil
}

func (s *omnistoreStorage) putSingle(ctx context.Context, key string, data []byte, getters ...AttrGetter) error {

	// Store a one-byte reserved marker for an empty logical object. The marker
	// is translated back to a zero-size object by Get, Head and List.
	if len(data) == 0 {
		n, err := s.client.WriteAt(ctx, s.emptyRef(key), 0, []byte{0})
		if err != nil {
			return mapOmniError(err)
		}
		if n != 1 {
			return fmt.Errorf("omnistore put %q: short empty marker write: got %d, want 1", key, n)
		}
		if err := s.deleteRef(ctx, s.ref(key)); err != nil {
			return mapOmniError(err)
		}
		if err := s.deleteRef(ctx, s.manifestRef(key)); err != nil {
			return mapOmniError(err)
		}
		attrs := ApplyGetters(getters...)
		if attrs.requestSize != nil {
			*attrs.requestSize = 0
		}
		return nil
	}

	// The JuiceFS block key contains the logical block size. For the normal
	// uncompressed path, a replacement either has the same size or gets a new
	// key and the old key is deleted by JuiceFS. When a direct caller reuses a
	// key with a smaller payload, publish a new immutable part through a
	// manifest instead of leaving stale tail bytes in the old Chunk.
	if info, statErr := s.client.Stat(ctx, s.ref(key)); statErr == nil && int64(info.Size) > int64(len(data)) {
		return s.putVersionedSingle(ctx, key, data, getters...)
	} else if statErr != nil && !errors.Is(statErr, omnistore.ErrNotFound) {
		return mapOmniError(statErr)
	}

	n, err := s.client.WriteAt(ctx, s.ref(key), 0, data)
	if err != nil {
		return mapOmniError(err)
	}
	if n != len(data) {
		return fmt.Errorf("omnistore put %q: short write: got %d, want %d", key, n, len(data))
	}
	// Publish the non-empty representation only after its data is written.
	// If a previous empty marker cannot be removed, keep returning the error so
	// the caller retries instead of observing a stale empty object indefinitely.
	if err := s.deleteRef(ctx, s.emptyRef(key)); err != nil {
		return mapOmniError(err)
	}
	if err := s.deleteRef(ctx, s.manifestRef(key)); err != nil {
		return mapOmniError(err)
	}
	attrs := ApplyGetters(getters...)
	if attrs.requestSize != nil {
		*attrs.requestSize = int64(n)
	}
	return nil
}

func (s *omnistoreStorage) putVersionedSingle(ctx context.Context, key string, data []byte, getters ...AttrGetter) error {
	generation := nextOmniGeneration()
	partRef := s.partRef(key, generation, 0)
	n, err := s.client.WriteAt(ctx, partRef, 0, data)
	if err != nil {
		return mapOmniError(err)
	}
	if n != len(data) {
		return fmt.Errorf("omnistore put %q: short versioned write: got %d, want %d", key, n, len(data))
	}
	manifest := encodeOmniManifest(omniManifest{generation: generation, size: uint64(len(data)), partCount: 1})
	n, err = s.client.WriteAt(ctx, s.manifestRef(key), 0, manifest)
	if err != nil {
		return mapOmniError(err)
	}
	if n != len(manifest) {
		return fmt.Errorf("omnistore put %q: short versioned manifest write: got %d, want %d", key, n, len(manifest))
	}
	if err := s.deleteRef(ctx, s.ref(key)); err != nil {
		return mapOmniError(err)
	}
	if err := s.deleteRef(ctx, s.emptyRef(key)); err != nil {
		return mapOmniError(err)
	}
	attrs := ApplyGetters(getters...)
	if attrs.requestSize != nil {
		*attrs.requestSize = int64(len(data))
	}
	return nil
}

func (s *omnistoreStorage) putFragmented(ctx context.Context, key string, first []byte, rest io.Reader, getters ...AttrGetter) error {
	generation := nextOmniGeneration()
	part := uint64(0)
	size := uint64(0)
	written := make([]omnistore.ChunkRef, 0, 2)
	cleanup := func() {
		for _, ref := range written {
			_ = s.deleteRef(context.Background(), ref)
		}
	}
	writePart := func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		ref := s.partRef(key, generation, part)
		n, err := s.client.WriteAt(ctx, ref, 0, data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return fmt.Errorf("omnistore put %q: short part write: got %d, want %d", key, n, len(data))
		}
		written = append(written, ref)
		size += uint64(n)
		part++
		return nil
	}

	if err := writePart(first); err != nil {
		cleanup()
		return mapOmniError(err)
	}
	buffer := make([]byte, int(omnistore.MaxChunkSize))
	for {
		n, err := io.ReadFull(rest, buffer)
		if n > 0 {
			if writeErr := writePart(buffer[:n]); writeErr != nil {
				cleanup()
				return mapOmniError(writeErr)
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		cleanup()
		return err
	}

	manifest := omniManifest{generation: generation, size: size, partCount: part}
	encoded := encodeOmniManifest(manifest)
	n, err := s.client.WriteAt(ctx, s.manifestRef(key), 0, encoded)
	if err != nil {
		cleanup()
		return mapOmniError(err)
	}
	if n != len(encoded) {
		cleanup()
		return fmt.Errorf("omnistore put %q: short manifest write: got %d, want %d", key, n, len(encoded))
	}

	// The manifest is the publication point. Older generations are retained
	// until logical Delete so a GET that already loaded an older manifest never
	// observes its parts disappearing during an overwrite.
	if err := s.deleteRef(ctx, s.ref(key)); err != nil {
		return mapOmniError(err)
	}
	if err := s.deleteRef(ctx, s.emptyRef(key)); err != nil {
		return mapOmniError(err)
	}
	attrs := ApplyGetters(getters...)
	if attrs.requestSize != nil {
		*attrs.requestSize = int64(size)
	}
	return nil
}

func (s *omnistoreStorage) getFragmented(ctx context.Context, key string, manifest omniManifest, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	size := int64(manifest.size)
	if off >= size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := size
	if limit > 0 && off+limit < end {
		end = off + limit
	}
	data := make([]byte, end-off)
	chunkSize := uint64(omnistore.MaxChunkSize)
	firstPart := uint64(off) / chunkSize
	lastPart := uint64(end-1) / chunkSize
	for part := firstPart; part <= lastPart; part++ {
		partStart := part * chunkSize
		readStart := uint64(0)
		if uint64(off) > partStart {
			readStart = uint64(off) - partStart
		}
		readEnd := chunkSize
		if uint64(end)-partStart < readEnd {
			readEnd = uint64(end) - partStart
		}
		partData := data[partStart+readStart-uint64(off) : partStart+readEnd-uint64(off)]
		n, err := s.client.ReadAt(ctx, s.partRef(key, manifest.generation, part), uint32(readStart), partData)
		if err != nil {
			return nil, mapOmniError(err)
		}
		if n != len(partData) {
			return nil, fmt.Errorf("omnistore get %q: short part read: got %d, want %d", key, n, len(partData))
		}
	}
	attrs := ApplyGetters(getters...)
	if attrs.requestSize != nil {
		*attrs.requestSize = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *omnistoreStorage) Delete(ctx context.Context, key string, _ ...AttrGetter) error {
	manifest, err := s.loadManifest(ctx, key)
	if err != nil {
		return mapOmniError(err)
	}

	refs := []omnistore.ChunkRef{s.ref(key), s.emptyRef(key), s.manifestRef(key), s.mtimeRef(key)}
	if manifest != nil {
		for part := uint64(0); part < manifest.partCount; part++ {
			refs = append(refs, s.partRef(key, manifest.generation, part))
		}
	}

	var firstErr error
	for _, ref := range refs {
		if err := s.deleteRef(ctx, ref); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// A failed or superseded fragmented PUT can leave retired generations
	// behind. Delete is the safe point at which those hidden parts can be
	// removed as well; List never exposes them as logical objects.
	partPrefix := s.prefix + key + partObjectSuffix
	marker := ""
	for {
		entries, hasMore, next, listErr := s.client.ListChunks(ctx, s.poolID, partPrefix, marker, 256)
		if listErr != nil {
			if firstErr == nil {
				firstErr = listErr
			}
			break
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.ChunkID, partPrefix) {
				continue
			}
			if deleteErr := s.deleteRef(ctx, omnistore.ChunkRef{PoolID: s.poolID, ChunkID: entry.ChunkID}); deleteErr != nil && firstErr == nil {
				firstErr = deleteErr
			}
		}
		if !hasMore {
			break
		}
		if next == "" || next == marker {
			if firstErr == nil {
				firstErr = fmt.Errorf("omnistore delete %q: part list marker did not advance", key)
			}
			break
		}
		marker = next
	}
	s.stateMu.Lock()
	for uploadID, upload := range s.uploads {
		if upload.key == key {
			delete(s.uploads, uploadID)
		}
	}
	result := mapOmniError(firstErr)
	if result == nil {
		delete(s.mtimes, key)
	}
	s.stateMu.Unlock()
	return result
}

func (s *omnistoreStorage) deleteRef(ctx context.Context, ref omnistore.ChunkRef) error {
	// Delete is a Raft-backed operation in OmniStore. Avoid submitting a
	// no-op delete for the reserved representations that are absent on the
	// normal single-Chunk path; unlike an S3 delete, an absent OmniStore Chunk
	// would otherwise still consume a PG log entry and wait for replication.
	if _, err := s.client.Stat(ctx, ref); err != nil {
		if errors.Is(err, omnistore.ErrNotFound) {
			return nil
		}
		return err
	}
	err := s.client.Delete(ctx, ref)
	if errors.Is(err, omnistore.ErrNotFound) {
		return nil
	}
	return err
}

func (s *omnistoreStorage) Head(ctx context.Context, key string) (Object, error) {
	manifest, err := s.loadManifest(ctx, key)
	if err != nil {
		return nil, mapOmniError(err)
	}
	if manifest != nil {
		mtime, mtimeErr := s.mtimeFor(ctx, key)
		if mtimeErr != nil {
			return nil, mtimeErr
		}
		return &obj{
			key:    key,
			size:   int64(manifest.size),
			mtime:  mtime,
			isDir:  strings.HasSuffix(key, "/"),
			status: "",
		}, nil
	}
	if _, err := s.client.Stat(ctx, s.emptyRef(key)); err == nil {
		mtime, mtimeErr := s.mtimeFor(ctx, key)
		if mtimeErr != nil {
			return nil, mtimeErr
		}
		return &obj{
			key:    key,
			size:   0,
			mtime:  mtime,
			isDir:  strings.HasSuffix(key, "/"),
			status: "",
		}, nil
	} else if !errors.Is(err, omnistore.ErrNotFound) {
		return nil, mapOmniError(err)
	}
	info, err := s.client.Stat(ctx, s.ref(key))
	if err != nil {
		return nil, mapOmniError(err)
	}
	mtime, mtimeErr := s.mtimeFor(ctx, key)
	if mtimeErr != nil {
		return nil, mtimeErr
	}
	return &obj{
		key:    key,
		size:   int64(info.Size),
		mtime:  mtime,
		isDir:  strings.HasSuffix(key, "/"),
		status: "",
	}, nil
}

// visibleEntry converts one physical Chunk entry into a logical object. Part
// Chunks are intentionally hidden. A manifest wins over a stale single-Chunk
// representation with the same logical key, which keeps interrupted or
// cross-process overwrites from producing duplicate objects in List.
func (s *omnistoreStorage) visibleEntry(ctx context.Context, entry omnistore.ChunkEntry) (string, int64, bool, error) {
	key, ok := s.logicalKey(entry.ChunkID)
	if !ok {
		return "", 0, false, nil
	}
	if strings.HasSuffix(key, mtimeObjectSuffix) {
		return "", 0, false, nil
	}
	if isOmniPartKey(key) {
		return "", 0, false, nil
	}
	if strings.HasSuffix(key, manifestObjectSuffix) {
		key = strings.TrimSuffix(key, manifestObjectSuffix)
		manifest, err := s.loadManifest(ctx, key)
		if err != nil {
			return "", 0, false, err
		}
		if manifest == nil {
			return "", 0, false, nil
		}
		return key, int64(manifest.size), true, nil
	}
	if manifest, err := s.loadManifest(ctx, key); err != nil {
		return "", 0, false, err
	} else if manifest != nil {
		return "", 0, false, nil
	}
	if strings.HasSuffix(key, emptyObjectSuffix) {
		return strings.TrimSuffix(key, emptyObjectSuffix), 0, true, nil
	}
	return key, int64(entry.Size), true, nil
}

func (s *omnistoreStorage) List(ctx context.Context, prefix, startAfter, token, delimiter string,
	limit int64, _ bool) ([]Object, bool, string, error) {
	if limit <= 0 || limit > int64(^uint32(0)) {
		return nil, false, "", fmt.Errorf("omnistore list: invalid limit %d", limit)
	}
	cursor := token
	if cursor == "" {
		cursor = s.prefix + startAfter
	}
	startAfterKey := startAfter
	if s.prefix != "" {
		startAfterKey = strings.TrimPrefix(startAfterKey, s.prefix)
	}
	physicalPrefix := s.prefix + prefix

	// With no delimiter, the SDK returns globally ordered physical entries and
	// an exclusive continuation marker. Walk as many physical pages as needed
	// because one logical large object can have many hidden part Chunks.
	if delimiter == "" {
		objects := make([]Object, 0, int(limit))
		seen := make(map[string]int)
		lastVisibleMarker := ""
		for {
			entries, hasMore, next, err := s.client.ListChunks(ctx, s.poolID, physicalPrefix, cursor, uint32(limit))
			if err != nil {
				return nil, false, "", mapOmniError(err)
			}
			for _, entry := range entries {
				entryMarker := entry.ChunkID
				key, size, visible, entryErr := s.visibleEntry(ctx, entry)
				if entryErr != nil {
					return nil, false, "", mapOmniError(entryErr)
				}
				if !visible {
					continue
				}
				if token == "" && startAfterKey != "" && key <= startAfterKey {
					lastVisibleMarker = entryMarker
					continue
				}
				if index, exists := seen[key]; exists {
					// A manifest is preferred over a stale ordinary entry;
					// visibleEntry already hides the latter in the usual case.
					objects[index].(*obj).size = size
					lastVisibleMarker = entryMarker
					continue
				}
				if int64(len(objects)) >= limit {
					marker := lastVisibleMarker
					if marker == "" {
						marker = cursor
					}
					if marker == "" {
						return nil, false, "", fmt.Errorf("omnistore list: continuation marker did not advance")
					}
					return objects, true, marker, nil
				}
				mtime, mtimeErr := s.mtimeFor(ctx, key)
				if mtimeErr != nil {
					return nil, false, "", mapOmniError(mtimeErr)
				}
				seen[key] = len(objects)
				objects = append(objects, &obj{
					key:    key,
					size:   size,
					mtime:  mtime,
					isDir:  strings.HasSuffix(key, "/"),
					status: "",
				})
				lastVisibleMarker = entryMarker
			}
			if !hasMore {
				return objects, false, "", nil
			}
			if next == "" || next == cursor {
				return nil, false, "", fmt.Errorf("omnistore list: continuation marker did not advance")
			}
			cursor = next
		}
	}

	// A delimiter exposes virtual directories. The raw list is still fetched
	// by object key; collapse entries sharing the first delimiter component and
	// keep the raw continuation marker opaque to the caller. Fetch one raw entry
	// at a time so a raw page can never contain an unreturned entry after the
	// visible page limit. This is intentionally conservative: correctness of
	// continuation tokens matters more than batching for this MVP path.
	objects := make([]Object, 0, limit)
	seen := make(map[string]struct{})
	lastVisible := ""
	processedMarker := cursor
	if token != "" {
		if marker, visible, ok := decodeDelimiterListToken(token); ok {
			cursor = marker
			processedMarker = marker
			lastVisible = visible
			seen[visible] = struct{}{}
		}
	}
	for {
		entryCursor := cursor
		entries, hasMore, next, err := s.client.ListChunks(ctx, s.poolID, physicalPrefix, cursor, 1)
		if err != nil {
			return nil, false, "", mapOmniError(err)
		}
		for _, entry := range entries {
			key, size, visible, entryErr := s.visibleEntry(ctx, entry)
			if entryErr != nil {
				return nil, false, "", mapOmniError(entryErr)
			}
			if !visible {
				processedMarker = entry.ChunkID
				continue
			}
			if token == "" && startAfterKey != "" && key <= startAfterKey {
				processedMarker = entry.ChunkID
				continue
			}
			isDir := strings.HasSuffix(key, "/")
			remaining := strings.TrimPrefix(key, prefix)
			if pos := strings.Index(remaining, delimiter); pos >= 0 {
				key = prefix + remaining[:pos+len(delimiter)]
				size = 0
				isDir = true
			}
			if _, exists := seen[key]; exists {
				processedMarker = entry.ChunkID
				continue
			}
			if len(objects) >= int(limit) {
				marker := processedMarker
				if marker == "" {
					marker = entryCursor
				}
				if marker == "" {
					return nil, false, "", fmt.Errorf("omnistore list: continuation marker is empty")
				}
				return objects, true, encodeDelimiterListToken(marker, lastVisible), nil
			}
			mtime, mtimeErr := s.mtimeFor(ctx, key)
			if mtimeErr != nil {
				return nil, false, "", mapOmniError(mtimeErr)
			}
			seen[key] = struct{}{}
			objects = append(objects, &obj{
				key:    key,
				size:   size,
				mtime:  mtime,
				isDir:  isDir,
				status: "",
			})
			lastVisible = key
			processedMarker = entry.ChunkID
		}

		if !hasMore {
			return objects, false, "", nil
		}
		if next == "" || next == cursor {
			return nil, false, "", fmt.Errorf("omnistore list: continuation marker did not advance")
		}
		cursor = next
	}
}

func (s *omnistoreStorage) Shutdown() {
	if s != nil && s.client != nil {
		_ = s.client.Close()
	}
}

func mapOmniError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, omnistore.ErrNotFound) {
		return os.ErrNotExist
	}
	return err
}

type omniStoreEndpoint struct {
	monitors []string
	poolID   uint64
	prefix   string
}

// parseOmniStoreEndpoint parses the MVP endpoint format:
//
//	omnistore://pool-name[/key-prefix]?pool-id=1&mon=host1:3333,host2:3333
//
// The pool name is currently informational. pool-id is required because the
// current SDK does not provide a pool-name lookup API. The optional path is a
// key namespace used to isolate direct ObjectStorage users such as `juicefs
// sync`; the normal JuiceFS mount path adds its volume prefix with WithPrefix.
// mon/monitor/monitors may be repeated and may contain comma-separated Monitor
// addresses.
func parseOmniStoreEndpoint(endpoint string) (omniStoreEndpoint, error) {
	if endpoint == "" {
		return omniStoreEndpoint{}, fmt.Errorf("omnistore endpoint is empty")
	}
	raw := endpoint
	if !strings.Contains(raw, "://") {
		raw = "omnistore://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return omniStoreEndpoint{}, fmt.Errorf("parse omnistore endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "omnistore" {
		return omniStoreEndpoint{}, fmt.Errorf("unsupported omnistore endpoint scheme %q", u.Scheme)
	}

	q := u.Query()
	poolValue := q.Get("pool-id")
	if poolValue == "" {
		poolValue = q.Get("pool_id")
	}
	if poolValue == "" {
		return omniStoreEndpoint{}, fmt.Errorf("omnistore endpoint requires pool-id")
	}
	poolID, err := strconv.ParseUint(poolValue, 10, 64)
	if err != nil || poolID == 0 {
		return omniStoreEndpoint{}, fmt.Errorf("invalid omnistore pool-id %q", poolValue)
	}

	var monitors []string
	for _, name := range []string{"mon", "monitor", "monitors"} {
		for _, value := range q[name] {
			for _, monitor := range strings.Split(value, ",") {
				monitor = strings.TrimSpace(monitor)
				if monitor != "" {
					monitors = append(monitors, monitor)
				}
			}
		}
	}
	if len(monitors) == 0 {
		return omniStoreEndpoint{}, fmt.Errorf("omnistore endpoint requires at least one mon")
	}

	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		prefix += "/"
	}
	return omniStoreEndpoint{monitors: monitors, poolID: poolID, prefix: prefix}, nil
}

func newOmniStore(endpoint, _accessKey, _secretKey, _token string) (ObjectStorage, error) {
	parsed, err := parseOmniStoreEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	client, err := omnistore.New(omnistore.Config{
		Monitors: parsed.monitors,
		PoolID:   parsed.poolID,
		// The Docker AIO backend can take longer than the SDK's generic
		// 10-second default to commit a PG entry. Keep one request alive long
		// enough to receive its result instead of retrying the same mutation
		// while the first Raft entry is still being applied.
		RequestTimeout: 60 * time.Second,
		// Monitor/OSD placement can briefly race with a PG leader change.
		// Retry transient leader and PG state errors at the SDK boundary so
		// JuiceFS does not turn a normal election window into an I/O error.
		MaxRetries: 5,
	})
	if err != nil {
		return nil, fmt.Errorf("create omnistore client: %w", err)
	}
	return &omnistoreStorage{
		client:   client,
		poolID:   parsed.poolID,
		prefix:   parsed.prefix,
		endpoint: endpoint,
		mtimes:   make(map[string]time.Time),
		uploads:  make(map[string]*omniMultipartUpload),
	}, nil
}

func init() {
	Register("omnistore", newOmniStore)
}
