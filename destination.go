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

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig

	index *pinecone.IndexConnection
}

type DestinationConfig struct {
	// APIKey is the API Key for authenticating with Pinecone.
	APIKey string `json:"apiKey" validate:"required"`

	// Host is the whole Pinecone index host URL.
	Host string `json:"host" validate:"required"`

	// Namespace is the Pinecone's index namespace. Defaults to the empty
	// namespace.
	Namespace string `json:"namespace"`
}

func (d DestinationConfig) toMap() map[string]string {
	return map[string]string{
		"apiKey":    d.APIKey,
		"host":      d.Host,
		"namespace": d.Namespace,
	}
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	if err := sdk.Util.ParseConfig(cfg, &d.config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("configured pinecone destination")

	return nil
}

func (d *Destination) Open(ctx context.Context) (err error) {
	d.index, err = newIndex(ctx, d.config)
	if err != nil {
		return fmt.Errorf("error creating a new writer: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("created pinecone destination")

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	batches, err := buildBatches(records)
	if err != nil {
		return 0, err
	}

	var written int
	for _, batch := range batches {
		batchWrittenRecs, err := batch.writeBatch(ctx, d.index)
		written += batchWrittenRecs
		if err != nil {
			return written, fmt.Errorf("failed to write record batch: %w", err)
		}
	}

	return written, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Pinecone Destination...")

	if err := d.index.Close(); err != nil {
		return fmt.Errorf("failed to close index: %w", err)
	}
	return nil
}

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

func vectorID(key sdk.Data) string {
	return string(key.Bytes())
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

func (s sparseValues) PineconeSparseValues() *pinecone.SparseValues {
	return &pinecone.SparseValues{
		Indices: s.Indices,
		Values:  s.Values,
	}
}

type pineconeVectorValues struct {
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values,omitempty"`
}

func parsePineconeVector(rec sdk.Record) (*pinecone.Vector, error) {
	id := vectorID(rec.Key)

	var vectorValues pineconeVectorValues
	err := json.Unmarshal(rec.Payload.After.Bytes(), &vectorValues)
	if err != nil {
		return nil, fmt.Errorf("failed to parse record json: %w", err)
	}

	structMap := make(map[string]any)
	for key, value := range rec.Metadata {
		structMap[key] = value
	}

	metadata, err := structpb.NewStruct(structMap)
	if err != nil {
		return nil, fmt.Errorf("error protobuf struct: %w", err)
	}

	vec := &pinecone.Vector{
		//revive:disable-next-line
		Id:           id,
		Values:       vectorValues.Values,
		SparseValues: vectorValues.SparseValues.PineconeSparseValues(),
		Metadata:     metadata,
	}

	return vec, nil
}
