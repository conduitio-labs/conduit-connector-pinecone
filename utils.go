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
	"net/url"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

func newIndex(ctx context.Context, config DestinationConfig) (*pinecone.IndexConnection, error) {
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.APIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	hostURL, err := url.Parse(config.Host)
	if err != nil {
		return nil, fmt.Errorf("invalid host url: %w", err)
	}

	var index *pinecone.IndexConnection
	if config.Namespace != "" {
		index, err = client.IndexWithNamespace(hostURL.Host, config.Namespace)
		if err != nil {
			return nil, fmt.Errorf(
				"error establishing index connection to namespace %v: %w",
				config.Namespace, err)
		}
	} else {
		index, err = client.Index(hostURL.Host)
		if err != nil {
			return nil, fmt.Errorf("error establishing index connection: %w", err)
		}
	}
	sdk.Logger(ctx).Info().Msg("created pinecone index")

	return index, nil
}

func recordID(key sdk.Data) string {
	return string(key.Bytes())
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

type pineconeVector struct {
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values,omitempty"`
}

func (r pineconeVector) PineconeSparseValues() *pinecone.SparseValues {
	// the used pinecone go client needs a nil pointer when no sparse values given, or else it
	// will throw a "Sparse vector must contain at least one value" error
	if len(r.SparseValues.Indices) == 0 && len(r.SparseValues.Values) == 0 {
		return nil
	}

	v := &pinecone.SparseValues{
		Indices: r.SparseValues.Indices,
		Values:  r.SparseValues.Values,
	}
	return v
}

func parsePineconeVector(rec sdk.Record) (*pinecone.Vector, error) {
	id := recordID(rec.Key)

	payload, err := parseRecordPayload(rec)
	if err != nil {
		return nil, err
	}

	metadata, err := parsePineconeMetadata(rec)
	if err != nil {
		return nil, err
	}

	vec := &pinecone.Vector{
		//revive:disable-next-line
		Id:           id,
		Values:       payload.Values,
		SparseValues: payload.PineconeSparseValues(),
		Metadata:     metadata,
	}

	return vec, nil
}

func parseRecordPayload(rec sdk.Record) (parsed pineconeVector, err error) {
	data := rec.Payload.After

	if data == nil || len(data.Bytes()) == 0 {
		return parsed, errors.New("empty payload")
	}

	err = json.Unmarshal(data.Bytes(), &parsed)
	if err != nil {
		return parsed, fmt.Errorf("error unmarshalling JSON: %w", err)
	}

	return parsed, nil
}

func parsePineconeMetadata(rec sdk.Record) (*pinecone.Metadata, error) {
	convertedMap := make(map[string]any)
	for key, value := range rec.Metadata {
		convertedMap[key] = value
	}

	metadata, err := structpb.NewStruct(convertedMap)
	if err != nil {
		return nil, fmt.Errorf("error creating metadata: %w", err)
	}

	return metadata, nil
}
