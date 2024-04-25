package pinecone

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

func (d *Destination) Open(ctx context.Context) error {
	newWriter, err := NewWriter(ctx, d.config)
	if err != nil {
		return fmt.Errorf("error creating a new writer: %w", err)
	}
	d.writer = newWriter

	sdk.Logger(ctx).Info().Msg("created pinecone destination")

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Upsert,
			d.writer.Upsert,
			d.writer.Delete,
			d.writer.Upsert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
		sdk.Logger(ctx).Trace().Msgf("wrote record op %s", record.Operation.String())
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Pinecone Destination...")

	return d.writer.Close()
}
