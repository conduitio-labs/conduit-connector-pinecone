// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/matryer/is"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

func destConfigFromEnv(t *testing.T) DestinationConfig {
	return DestinationConfig{
		APIKey: requiredEnv(t, "API_KEY"),
		Host:   requiredEnv(t, "HOST_URL"),
	}
}

func requiredEnv(t *testing.T, key string) string {
	val := os.Getenv(key)
	if val == "" {
		t.Fatalf("env var %v unset", key)
	}

	return val
}

func TestDestination_NamespaceSet(t *testing.T) {
	ctx := context.Background()
	destCfg := destConfigFromEnv(t)
	// we use the default namespace on all other tests, so it's correct to set it here
	destCfg.Namespace = "test-namespace"

	is := is.New(t)
	dest := newDestination()

	err := dest.Configure(ctx, destCfg.toMap())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	defer teardown(ctx, is, dest)

	is.Equal(dest.writer.index.Namespace, "test-namespace")
}

func createIndex(is *is.I, destCfg DestinationConfig) *pinecone.IndexConnection {
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: destCfg.APIKey,
	})
	is.NoErr(err)

	index, err := client.Index(destCfg.Host)
	is.NoErr(err)

	return index
}

func TestDestination_Integration_WriteDelete(t *testing.T) {
	ctx := context.Background()
	destCfg := destConfigFromEnv(t)
	is := is.New(t)
	dest := newDestination()

	err := dest.Configure(ctx, destCfg.toMap())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	defer teardown(ctx, is, dest)

	id := uuid.NewString()
	position := sdk.Position(fmt.Sprintf("pos-%v", id))
	metadata := map[string]string{
		"pinecone.prop1": "val1",
		"pinecone.prop2": "val2",
	}

	vecsToBeWritten := recordPayload{
		Values: []float32{1, 2},
		SparseValues: sparseValues{
			Indices: []uint32{3, 5},
			Values:  []float32{0.5, 0.3},
		},
	}

	payload, err := json.Marshal(vecsToBeWritten)
	is.NoErr(err)

	rec := sdk.Util.Source.NewRecordCreate(position, metadata, sdk.RawData(id), sdk.RawData(payload))
	_, err = dest.Write(ctx, []sdk.Record{rec})
	is.NoErr(err)

	index := createIndex(is, destCfg)

	assertWrittenRecordIndex(ctx, t, is, index, id, vecsToBeWritten)

	rec = sdk.Util.Source.NewRecordDelete(position, metadata, sdk.RawData(id))
	_, err = dest.Write(ctx, []sdk.Record{rec})
	is.NoErr(err)

	assertDeletedRecordIndex(ctx, t, is, index, id)

	deleteAllRecords(is, dest.writer.index)
}

const maxRetries = 4

func waitTime(i int) time.Duration {
	wait := math.Pow(2, float64(i))
	return time.Duration(wait) * time.Second
}

func assertWrittenRecordIndex(ctx context.Context, t *testing.T, is *is.I, index *pinecone.IndexConnection, id string, writtenVecs recordPayload) {
	// Pinecone writes appear to be asynchronous. At the very least, in the current free tier serverless
	// configuration that I've tested, pinecone writes occurred slightly after the RPC call
	// returned data. Therefore, the following retry logic is needed to make tests more robust
	for i := 1; i <= maxRetries; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		vec, ok := res.Vectors[id]
		if !ok {
			if i == maxRetries {
				is.Fail() // vector was not written
			} else {
				wait := waitTime(i)
				t.Logf("retrying with wait of %v", wait)
				time.Sleep(wait)
				continue
			}
		}

		is.Equal(vec.Values, writtenVecs.Values)
		is.Equal(vec.SparseValues.Values, writtenVecs.SparseValues.Values)
		is.Equal(vec.SparseValues.Indices, writtenVecs.SparseValues.Indices)
		break
	}
}

func assertDeletedRecordIndex(ctx context.Context, t *testing.T, is *is.I, index *pinecone.IndexConnection, id string) {
	// same as assertWrittenRecordIndex, we need the retry for robustness
	for i := 0; i <= maxRetries; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		_, ok := res.Vectors[id]
		if ok {
			if i == maxRetries {
				is.Fail() // vector found, not properly deleted
			} else {
				wait := waitTime(i)
				t.Logf("retrying with wait of %v", wait)
				time.Sleep(wait)
				continue
			}
		}
	}
}

func deleteAllRecords(is *is.I, index *pinecone.IndexConnection) {
	ctx := context.Background()
	err := index.DeleteAllVectorsInNamespace(&ctx)
	is.NoErr(err)
}

func TestMain(t *testing.M) {
	godotenv.Load()

	t.Run()
}

type connectorResource interface {
	Teardown(ctx context.Context) error
}

func teardown(ctx context.Context, is *is.I, resource connectorResource) {
	err := resource.Teardown(ctx)
	is.NoErr(err)
}
