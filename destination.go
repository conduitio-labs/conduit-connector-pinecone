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
	"strings"
	"text/template"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig

	colWriter collectionWriter
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

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) (err error) {
	if err = sdk.Util.ParseConfig(cfg, &d.config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("configured pinecone destination")

	return nil
}

func (d *Destination) Open(ctx context.Context) (err error) {
	if isGoTextTemplate(d.config.Namespace) {
		template, err := template.New("collection").Parse(d.config.Namespace)
		if err != nil {
			return fmt.Errorf("failed to parse namespace template %s: %w", d.config.Namespace, err)
		}
		d.colWriter = newMulticollectionWriter(d.config.APIKey, d.config.Host, template)

	} else if d.config.Namespace == "" {
		d.colWriter = newMulticollectionWriter(d.config.APIKey, d.config.Host, nil)
	} else {

		index, err := newIndex(ctx, newIndexParams{
			apiKey:    d.config.APIKey,
			host:      d.config.Host,
			namespace: d.config.Namespace,
		})
		if err != nil {
			return fmt.Errorf("error creating a new writer: %w", err)
		}

		d.colWriter = &singleCollectionWriter{index: index}
	}

	sdk.Logger(ctx).Info().Msg("created pinecone destination")

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	return d.colWriter.writeRecords(ctx, records)
}

func (d *Destination) Teardown(ctx context.Context) error {
	if err := d.colWriter.close(); err != nil {
		return fmt.Errorf("failed to close index: %w", err)
	}
	return nil
}

type newIndexParams struct {
	apiKey    string
	host      string
	namespace string
}

// newIndex creates a new connection to a given namespace. We don't pass the
// destination configuration because in multicollection mode the namespace is
// dynamic, and we assume that the DestinationConfig should be an immutable
// struct.
func newIndex(ctx context.Context, params newIndexParams) (*pinecone.IndexConnection, error) {
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: params.apiKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	hostURL, err := url.Parse(params.host)
	if err != nil {
		return nil, fmt.Errorf("invalid host url: %w", err)
	}

	var index *pinecone.IndexConnection
	if params.namespace != "" {
		index, err = client.IndexWithNamespace(hostURL.Host, params.namespace)
		if err != nil {
			return nil, fmt.Errorf(
				"error establishing index connection to namespace %v: %w",
				params.namespace, err)
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

func isGoTextTemplate(s string) bool {
	return strings.Contains(s, "{{") && strings.Contains(s, "}}")
}
