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
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.Client
	index  *pinecone.IndexConnection
}

func NewWriter(ctx context.Context, config DestinationConfig) (*Writer, error) {
	var w Writer
	var err error
	w.client, err = pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.APIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	// index urls should have their protocol trimmed
	host := strings.TrimPrefix(config.Host, "https://")

	if config.Namespace != "" {
		w.index, err = w.client.IndexWithNamespace(host, config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("error establishing index connection: %w", err)
		}
	} else {
		w.index, err = w.client.Index(host)
		if err != nil {
			return nil, fmt.Errorf("error establishing index connection: %w", err)
		}
	}
	sdk.Logger(ctx).Info().Msg("created pinecone index")

	return &w, nil
}

func (w *Writer) UpsertVectors(ctx context.Context, vectors []*pinecone.Vector) (error) {
	_, err := w.index.UpsertVectors(&ctx, vectors)
	if err != nil {
		return fmt.Errorf("error upserting record: %w", err)
	}

	return nil
}

func (w *Writer) DeleteRecords(ctx context.Context, vectorIds []string) error {
	err := w.index.DeleteVectorsById(&ctx, vectorIds)
	if err != nil {
		return fmt.Errorf("error deleting record: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	if err := w.index.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	return nil
}

func recordID(key sdk.Data) string {
	return string(key.Bytes())
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

type recordPayload struct {
	ID           string       `json:"id"`
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values,omitempty"`
}

func (r recordPayload) PineconeSparseValues() *pinecone.SparseValues {
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

func parseRecordPayload(payload sdk.Change) (parsed recordPayload, err error) {
	data := payload.After

	if data == nil || len(data.Bytes()) == 0 {
		return parsed, errors.New("empty payload")
	}

	err = json.Unmarshal(data.Bytes(), &parsed)
	if err != nil {
		return parsed, fmt.Errorf("error unmarshalling JSON: %w", err)
	}

	return parsed, nil
}

func recordMetadata(data sdk.Metadata) (*pinecone.Metadata, error) {
	convertedMap := make(map[string]any)
	for key, value := range data {
		if trimmed, hasPrefix := trimPineconeKey(key); hasPrefix {
			convertedMap[trimmed] = value
		}
	}
	metadata, err := structpb.NewStruct(convertedMap)
	if err != nil {
		return nil, fmt.Errorf("error creating metadata: %w", err)
	}

	return metadata, nil
}

var keyPrefix = "pinecone."

func trimPineconeKey(key string) (trimmed string, hasPrefix bool) {
	if strings.HasPrefix(key, keyPrefix) {
		return key[len(keyPrefix):], true
	}

	return key, false
}
