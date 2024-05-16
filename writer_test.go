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
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestRecordMetadata(t *testing.T) {
	is := is.New(t)

	var rec sdk.Record
	rec.Metadata = sdk.Metadata{
		"created_at":     "2023-03-15T14:25:07Z",
		"pinecone.prop1": "a1",
		"pinecone.prop2": "a2",
	}
	recMetadata, err := parsePineconeMetadata(rec)
	is.NoErr(err)

	is.Equal(recMetadata.Fields["prop1"].AsInterface().(string), "a1")
	is.Equal(recMetadata.Fields["prop2"].AsInterface().(string), "a2")
}

func testRecord(op sdk.Operation, keyStr string) sdk.Record {
	var position sdk.Position
	var key sdk.Data = sdk.RawData(keyStr)
	var metadata sdk.Metadata
	var payload sdk.Data = sdk.StructuredData{
		"vector": []float64{1, 2},
	}

	return sdk.Record{
		Position: position, Operation: op,
		Metadata: metadata, Key: key,
		Payload: sdk.Change{
			Before: nil,
			After:  payload,
		},
	}
}

func TestParseRecords(t *testing.T) {
	is := is.New(t)

	records := []sdk.Record{
		testRecord(sdk.OperationUpdate, "key1"),

		testRecord(sdk.OperationDelete, "key2"), testRecord(sdk.OperationDelete, "key3"),

		testRecord(sdk.OperationCreate, "key4"), testRecord(sdk.OperationCreate, "key5"),
		testRecord(sdk.OperationCreate, "key6"),

		testRecord(sdk.OperationDelete, "key7"),

		testRecord(sdk.OperationSnapshot, "key8"), testRecord(sdk.OperationSnapshot, "key9"),
	}

	batches, err := buildBatches(records)
	is.NoErr(err)

	is.Equal(len(batches), 5)

	assertUpsertBatch(is, batches[0], "key1")
	assertDeleteBatch(is, batches[1], "key2", "key3")
	assertUpsertBatch(is, batches[2], "key4", "key5", "key6")
	assertDeleteBatch(is, batches[3], "key7")
	assertUpsertBatch(is, batches[4], "key8", "key9")
}

func assertUpsertBatch(is *is.I, batch recordBatch, keys ...string) {
	upsertBatch, ok := batch.(upsertBatch)
	is.True(ok) // batch isn't upsertBatch

	is.Equal(len(upsertBatch.vectors), len(keys))

	for i, vec := range upsertBatch.vectors {
		is.Equal(vec.Id, keys[i])
	}
}

func assertDeleteBatch(is *is.I, batch recordBatch, keys ...string) {
	deleteBatch, ok := batch.(deleteBatch)
	is.True(ok) // batch isn't upsertBatch

	is.Equal(len(deleteBatch.ids), len(keys))

	for i, id := range deleteBatch.ids {
		is.Equal(id, keys[i])
	}
}
