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
	"math/rand"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func assertUpsertBatch(is *is.I, batch recordBatch, records []sdk.Record) {
	upsertBatch, ok := batch.(*upsertBatch)
	is.True(ok) // batch isn't upsertBatch

	for i, vec := range upsertBatch.vectors {
		rec := records[i]

		parsed, err := parsePineconeVector(rec)
		is.NoErr(err)

		is.Equal(vec, parsed)
	}
}

func assertDeleteBatch(is *is.I, batch recordBatch, records []sdk.Record) {
	deleteBatch, ok := batch.(*deleteBatch)
	is.True(ok) // batch isn't deleteBatch

	keys := make([]string, len(records))
	for i, rec := range records {
		keys[i] = string(rec.Key.Bytes())
	}

	is.Equal(deleteBatch.ids, keys)
}

func TestBuildBatchesWithoutCollection(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		is := is.New(t)
		var records []sdk.Record
		batches, err := buildBatches(records, false)
		is.NoErr(err)

		is.Equal(len(batches), 0)
	})

	t.Run("only delete", func(t *testing.T) {
		is := is.New(t)
		records := testRecords(sdk.OperationDelete)
		batches, err := buildBatches(records, false)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertDeleteBatch(is, batches[0], records)
	})

	t.Run("only non delete", func(t *testing.T) {
		is := is.New(t)
		records := testRecords(sdk.OperationCreate)
		batches, err := buildBatches(records, false)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertUpsertBatch(is, batches[0], records)
	})

	t.Run("multiple ops", func(t *testing.T) {
		is := is.New(t)
		var records []sdk.Record
		batch0 := testRecords(sdk.OperationUpdate)
		records = append(records, batch0...)

		batch1 := testRecords(sdk.OperationDelete)
		records = append(records, batch1...)

		batch2 := testRecords(sdk.OperationCreate)
		records = append(records, batch2...)

		batch3 := testRecords(sdk.OperationDelete)
		records = append(records, batch3...)

		batch4 := testRecords(sdk.OperationSnapshot)
		records = append(records, batch4...)

		batches, err := buildBatches(records, false)
		is.NoErr(err)

		is.Equal(len(batches), 5)

		assertUpsertBatch(is, batches[0], batch0)
		assertDeleteBatch(is, batches[1], batch1)
		assertUpsertBatch(is, batches[2], batch2)
		assertDeleteBatch(is, batches[3], batch3)
		assertUpsertBatch(is, batches[4], batch4)
	})
}

func TestBuildBatchesWithCollection(t *testing.T) {
	t.Run("some records without destination", func(t *testing.T) {
		is := is.New(t)

		var records []sdk.Record

		batch0 := testRecordsDestination(sdk.OperationUpdate, "")
		records = append(records, batch0...)

		batch1 := testRecords(sdk.OperationDelete)
		records = append(records, batch1...)

		batch2 := testRecordsDestination(sdk.OperationDelete, "namespace2")
		records = append(records, batch2...)

		batches, err := buildBatches(records, true)
		is.NoErr(err)

		assertUpsertBatch(is, batches[0], batch0)
		assertDeleteBatch(is, batches[1], batch1)
		assertDeleteBatch(is, batches[2], batch2)

	})

	t.Run("different destinations", func(t *testing.T) {
		is := is.New(t)

		var records []sdk.Record

		batch0 := testRecordsDestination(sdk.OperationUpdate, "")
		records = append(records, batch0...)

		batch1 := testRecordsDestination(sdk.OperationDelete, "namespace2")
		records = append(records, batch1...)

		batch2 := testRecordsDestination(sdk.OperationDelete, "namespace2")
		records = append(records, batch2...)

		batches, err := buildBatches(records, true)
		is.NoErr(err)

		assertUpsertBatch(is, batches[0], batch0)
		assertDeleteBatch(is, batches[1], batch1)
		assertDeleteBatch(is, batches[2], batch2)
	})
}

func testRecordsDestination(op sdk.Operation, namespace string) []sdk.Record {
	total := rand.Intn(10) + 1
	recs := make([]sdk.Record, total)

	for i := range total {
		position := sdk.Position(randString())
		key := sdk.RawData(randString())
		metadata := sdk.Metadata{
			randString(): randString(),
			randString(): randString(),
		}
		if namespace != "" {
			metadata.SetCollection(namespace)
		}

		var payload sdk.Data = sdk.StructuredData{
			"vector": []float64{1, 2},
		}

		rec := sdk.Record{
			Position: position, Operation: op,
			Metadata: metadata, Key: key,
			Payload: sdk.Change{
				Before: nil, // discarded, the Pinecone destination connector doesn't use this field
				After:  payload,
			},
		}
		recs[i] = rec
	}

	return recs
}

func testRecords(op sdk.Operation) []sdk.Record {
	return testRecordsDestination(op, "")
}

func randString() string { return uuid.NewString()[0:8] }
