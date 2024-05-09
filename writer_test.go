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
	recMetadata, err := parseRecordMetadata(rec)
	is.NoErr(err)

	is.Equal(recMetadata.Fields["prop1"].AsInterface().(string), "a1")
	is.Equal(recMetadata.Fields["prop2"].AsInterface().(string), "a2")
}

func testRecord(op sdk.Operation) sdk.Record {
	var position sdk.Position
	var key sdk.Data = sdk.RawData("key")
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

func batchSize(batch recordBatch) int {
	switch batch := batch.(type) {
	case upsertBatch:
		return len(batch.vectors)
	case deleteBatch:
		return len(batch.ids)
	}

	panic("invalid batch")
}

func TestParseRecords(t *testing.T) {
	is := is.New(t)

	records := []sdk.Record{
		testRecord(sdk.OperationUpdate),

		testRecord(sdk.OperationDelete), testRecord(sdk.OperationDelete),

		testRecord(sdk.OperationCreate), testRecord(sdk.OperationCreate),
		testRecord(sdk.OperationCreate),

		testRecord(sdk.OperationDelete),

		testRecord(sdk.OperationSnapshot), testRecord(sdk.OperationSnapshot),
	}

	batches, err := parseRecords(records)
	is.NoErr(err)

	is.Equal(len(batches), 5)

	is.Equal(batchSize(batches[0]), 1)
	is.Equal(batchSize(batches[1]), 2)
	is.Equal(batchSize(batches[2]), 3)
	is.Equal(batchSize(batches[3]), 1)
	is.Equal(batchSize(batches[4]), 2)
}
