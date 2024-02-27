package destination

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-pinecone/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	pinecone "github.com/nekomeowww/go-pinecone"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client pinecone.IndexClient
	writer writer.Writer
}

type Config struct {
	// Config includes parameters that are the same in the source and destination.
	*Config
	// DestinationConfigParam must be either yes or no (defaults to yes).
	DestinationConfigParam string `validate:"inclusion=yes|no" default:"yes"`
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.

	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	if err := sdk.Util.ParseConfig(cfg, &d.config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.client, err = pinecone.NewIndexClient(
		pinecone.WithIndexName("sample-movies"),
		pinecone.WithEnvironment("gcp-starter"),
		pinecone.WithProjectName("0ukv0hs"),
		pinecone.WithAPIKey(ApiKey),
	)

	return nil
}

func (d *Destination) Open(ctx context.Context) error {

	d.writer, err = writer.New(ctx, &d.client, writer.Params{
		DB:    db,
		Table: d.config.Table,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
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
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	return nil
}
