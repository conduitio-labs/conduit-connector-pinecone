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
	"github.com/pkg/errors"
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

func recordID(key sdk.Data) string {
	return string(key.Bytes())
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

type pineconeVectorValues struct {
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values,omitempty"`
}

func (r pineconeVectorValues) PineconeSparseValues() *pinecone.SparseValues {
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

	payload, err := parsePineconeVectorValues(rec)
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

func parsePineconeVectorValues(rec sdk.Record) (values pineconeVectorValues, err error) {
	data := rec.Payload.After

	if data == nil || len(data.Bytes()) == 0 {
		return values, errors.New("empty payload")
	}

	err = json.Unmarshal(data.Bytes(), &values)
	if err != nil {
		return values, fmt.Errorf("error unmarshalling JSON: %w", err)
	}

	return values, nil
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
