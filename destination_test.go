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
	"encoding/json"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParsePineconeVector(t *testing.T) {
	is := is.New(t)

	vecToBeWritten := pineconeVectorValues{
		Values: []float32{1, 2},
		SparseValues: sparseValues{
			Indices: []uint32{3, 5},
			Values:  []float32{0.5, 0.3},
		},
	}

	payload, err := json.Marshal(vecToBeWritten)
	is.NoErr(err)

	rec := sdk.Record{
		Position:  nil,
		Operation: sdk.OperationCreate,
		Metadata: map[string]string{
			"prop1": "val1",
			"prop2": "val2",
		},
		Key: sdk.RawData("key1"),
		Payload: sdk.Change{
			Before: nil,
			After:  sdk.RawData(payload),
		},
	}

	vec, err := parsePineconeVector(rec)
	is.NoErr(err)

	is.Equal(vec.Id, "key1")

	is.Equal(vec.Values, vecToBeWritten.Values)
	is.Equal(vec.SparseValues, vecToBeWritten.SparseValues.PineconeSparseValues())

	metadata := vec.Metadata.AsMap()
	is.Equal(metadata["prop1"], "val1")
	is.Equal(metadata["prop2"], "val2")
}
