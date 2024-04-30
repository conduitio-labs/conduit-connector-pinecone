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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig
	writer *Writer
}

type DestinationConfig struct {
	// APIKey is the API Key for authenticating with Pinecone.
	APIKey string `json:"pinecone.apiKey" validate:"required"`

	// Host is the Pinecone index host URL
	Host string `json:"pinecone.host" validate:"required"`

	// Namespace is the Pinecone's index namespace. Defaults to the empty
	// namespace.
	Namespace string `json:"pinecone.namespace"`
}

func (d DestinationConfig) toMap() map[string]string {
	return map[string]string{
		"pinecone.apiKey":    d.APIKey,
		"pinecone.host":      d.Host,
		"pinecone.namespace": d.Namespace,
	}
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func newDestination() *Destination {
	return &Destination{}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	if err := sdk.Util.ParseConfig(cfg, &d.config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("configured pinecone destination")

	return nil
}

func (d *Destination) Open(ctx context.Context) (err error) {
	d.writer, err = NewWriter(ctx, d.config)
	if err != nil {
		return fmt.Errorf("error creating a new writer: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("created pinecone destination")

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	var deleteIds []string
	var upsertVecs []*pinecone.Vector

	addVec := func(record sdk.Record) error {
		id := recordID(record.Key)

		payload, err := parseRecordPayload(record.Payload)
		if err != nil {
			return fmt.Errorf("error getting payload: %w", err)
		}

		metadata, err := recordMetadata(record.Metadata)
		if err != nil {
			return fmt.Errorf("error getting metadata: %w", err)
		}

		vec := &pinecone.Vector{
			//revive:disable-next-line
			Id:           id,
			Values:       payload.Values,
			SparseValues: payload.PineconeSparseValues(),
			Metadata:     metadata,
		}

		upsertVecs = append(upsertVecs, vec)
		return nil
	}

	for i, record := range records {
		var err error
		switch record.Operation {
		case sdk.OperationCreate:
			err = addVec(record)
		case sdk.OperationUpdate:
			err = addVec(record)
		case sdk.OperationDelete:
			id := string(record.Key.Bytes())
			deleteIds = append(deleteIds, id)
		case sdk.OperationSnapshot:
			err = addVec(record)
		}
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
		sdk.Logger(ctx).Trace().Msgf("wrote record op %s", record.Operation.String())
	}

	if len(upsertVecs) != 0 {
		d.writer.UpsertVectors(ctx, upsertVecs)
	}
	if len(deleteIds) != 0 {
		d.writer.DeleteRecords(ctx, deleteIds)
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Pinecone Destination...")

	return d.writer.Close()
}
