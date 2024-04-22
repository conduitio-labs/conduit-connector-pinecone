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
	// PineconeAPIKey is the API Key for authenticating with Pinecone.
	PineconeAPIKey string `json:"pinecone.apiKey" validate:"required"`

	// PineconeHostURL is the Pinecone index host URL
	PineconeHostURL string `json:"pinecone.hostURL" validate:"required"`
}

func (d DestinationConfig) toMap() map[string]string {
	return map[string]string{
		"pinecone.apiKey":  d.PineconeAPIKey,
		"pinecone.hostURL": d.PineconeAPIKey,
	}
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Pinecone Destination...")
	var config DestinationConfig

	if err := sdk.Util.ParseConfig(cfg, &config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	d.config.PineconeAPIKey = cfg["Pinecone API Key"]
	d.config.PineconeHostURL = cfg["Pinecone Host URL"]

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening a Pinecone Destination...")

	newWriter, err := NewWriter(ctx, d.config)
	if err != nil {
		return fmt.Errorf("error creating a new writer: %w", err)
	}
	d.writer = newWriter

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
		sdk.Logger(ctx).Trace().Msg("wrote record")
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Pinecone Destination...")
	

	return d.writer.Close()
}
