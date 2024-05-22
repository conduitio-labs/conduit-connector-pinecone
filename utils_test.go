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
		"created_at": "2023-03-15T14:25:07Z",
		"prop1":      "a1",
		"prop2":      "a2",
	}
	recMetadata, err := parsePineconeMetadata(rec)
	is.NoErr(err)

	is.Equal(recMetadata.Fields["prop1"].AsInterface().(string), "a1")
	is.Equal(recMetadata.Fields["prop2"].AsInterface().(string), "a2")
}

func testRecords(op sdk.Operation, keys ...string) []sdk.Record {
	recs := make([]sdk.Record, len(keys))
	for i := range recs {
		var position sdk.Position
		var key sdk.Data = sdk.RawData(keys[i])
		var metadata sdk.Metadata
		var payload sdk.Data = sdk.StructuredData{
			"vector": []float64{1, 2},
		}

		rec := sdk.Record{
			Position: position, Operation: op,
			Metadata: metadata, Key: key,
			Payload: sdk.Change{
				Before: nil,
				After:  payload,
			},
		}
		recs[i] = rec
	}

	return recs
}
