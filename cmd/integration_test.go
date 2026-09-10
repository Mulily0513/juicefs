/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const gatewayMeta = "redis://127.0.0.1:6379/14"
const gatewayVolume = "gateway-volume"
const gatewayAddr = "localhost:9008"
const webdavMeta = "redis://127.0.0.1:6379/15"
const webdavVolume = "webdav-volume"
const webdavAddr = "localhost:9009"

// integrationStorageBucket allows the integration fixture to exercise a real
// object backend without changing the default local-file test behavior. It is
// intentionally test-only: production commands still receive their storage
// configuration from the volume format.
func integrationStorageBucket(fallback string) string {
	if endpoint := os.Getenv("OMNISTORE_JUICEFS_CMD_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return fallback
}

func integrationStorageFlags(fallback string) []string {
	if os.Getenv("OMNISTORE_JUICEFS_CMD_ENDPOINT") != "" {
		return []string{"--storage", "omnistore", "--bucket", integrationStorageBucket(fallback)}
	}
	return []string{"--bucket", fallback}
}

func integrationMetadataURL(db int, fallback string) string {
	if base := os.Getenv("OMNISTORE_JUICEFS_CMD_METADATA"); base != "" {
		return strings.TrimRight(base, "/") + "/" + fmt.Sprint(db)
	}
	return fallback
}

func startGateway(t *testing.T) {
	metadata := integrationMetadataURL(14, gatewayMeta)
	opt, _ := redis.ParseURL(metadata)
	rdb := redis.NewClient(opt)
	_ = rdb.FlushDB(context.Background())
	testDir := t.TempDir()
	formatArgs := []string{"", "format"}
	formatArgs = append(formatArgs, integrationStorageFlags(testDir)...)
	formatArgs = append(formatArgs, metadata, gatewayVolume)
	if err := Main(formatArgs); err != nil {
		t.Fatalf("format failed: %s", err)
	}

	// must do reset, otherwise will panic
	ResetHttp()

	go func() {
		if err := Main([]string{"", "gateway", metadata, gatewayAddr, "--multi-buckets", "--keep-etag", "--object-tag", "--no-usage-report"}); err != nil {
			t.Errorf("gateway failed: %s", err)
		}
	}()
	time.Sleep(2 * time.Second)
}

func startWebdav(t *testing.T) {
	metadata := integrationMetadataURL(15, webdavMeta)
	opt, _ := redis.ParseURL(metadata)
	rdb := redis.NewClient(opt)
	_ = rdb.FlushDB(context.Background())
	testDir := t.TempDir()
	formatArgs := []string{"", "format"}
	formatArgs = append(formatArgs, integrationStorageFlags(testDir)...)
	formatArgs = append(formatArgs, metadata, webdavVolume)
	if err := Main(formatArgs); err != nil {
		t.Fatalf("format failed: %s", err)
	}

	// must do reset, otherwise will panic
	ResetHttp()

	go func() {
		os.Setenv("WEBDAV_USER", "root")
		os.Setenv("WEBDAV_PASSWORD", "1234")
		if err := Main([]string{"", "webdav", metadata, webdavAddr, "--no-usage-report"}); err != nil {
			t.Errorf("gateway failed: %s", err)
		}
	}()
	time.Sleep(2 * time.Second)
}

func TestIntegration(t *testing.T) {
	mountTemp(t, nil, nil, []string{"--enable-ioctl"})
	defer umountTemp(t)
	startGateway(t)
	startWebdav(t)
	_ = os.Chdir("../integration")
	makeArgs := strings.Fields(os.Getenv("OMNISTORE_JUICEFS_INTEGRATION_TARGETS"))
	makeCmd := exec.Command("make", makeArgs...)
	makeCmd.Env = append(os.Environ(), "MOUNT_POINT="+testMountPoint)
	out, err := makeCmd.CombinedOutput()
	if err != nil {
		t.Logf("std out:\n%s\n", string(out))
		t.Fatalf("std err failed with %s\n", err)
	} else {
		t.Logf("std out:\n%s\n", string(out))
	}
}
