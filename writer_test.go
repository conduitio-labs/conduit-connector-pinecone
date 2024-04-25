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
	"fmt"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestRecordMetadata(t *testing.T) {
	is := is.New(t)

	sdkMetadata := sdk.Metadata{
		"created_at":     "2023-03-15T14:25:07Z",
		"pinecone.prop1": "a1",
		"pinecone.prop2": "a2",
	}
	recMetadata, err := recordMetadata(sdkMetadata)
	is.NoErr(err)

	fmt.Println()

	is.Equal(recMetadata.Fields["prop1"].AsInterface().(string), "a1")
	is.Equal(recMetadata.Fields["prop2"].AsInterface().(string), "a2")
}
