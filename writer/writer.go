package writer

import (
	"context"
	"fmt"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nekomeowww/go-pinecone"
	"strings"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.IndexClient
}

// NewWriter New creates new instance of the Writer.
func NewWriter(ctx context.Context, client *pinecone.IndexClient) (*Writer, error) {

	pineconeClient, err := NewPineconeClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create pinecone pineconeClient: %v", err)
	}

	writer := &Writer{
		client: pineconeClient,
	}
	return writer, nil
}

func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {

	upsertParams := pinecone.UpsertVectorsParams{
		Vectors: []*pinecone.Vector{
			{
				ID:       "YOUR_VECTOR_ID",
				Values:   values,
				Metadata: map[string]any{"foo": "bar"},
			},
		},
	}

	UpsertResponse, err := client.UpsertVectors(ctx, upsertParams)
	if err != nil {
		fmt.Print(err)
	}

}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	keys, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	if len(keys) == 0 {
		return ErrNoKey
	}

	query, args := w.buildDeleteQuery(tableName, keys)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}

// NewPineconeClient takes the Pinecone Index URL string in Config and splits into respective parts to establish a
// connection
func NewPineconeClient(ctx context.Context) (*pinecone.IndexClient, error) {
	sdk.Logger(ctx).Trace().Msg("Creating a Pinecone Client...")

	urlWithoutProtocol := strings.TrimPrefix(d.config.PineconeHostURL, "https://")
	urlParts := strings.Split(urlWithoutProtocol, ".")

	if len(urlParts) < 4 {
		return nil, fmt.Errorf("URL does not conform to expected format")

	}

	// Extract the 'index-name-project-name' part and then further split it by '-'
	nameParts := strings.Split(urlParts[0], "-")
	if len(nameParts) < 2 {
		return nil, fmt.Errorf("URL does not conform to expected index-name-project-name format")
	}

	// Assuming the last part after splitting by '-' is the project name and the rest is the index name
	projectName := nameParts[len(nameParts)-1]
	indexName := strings.Join(nameParts[:len(nameParts)-1], "-")
	environment := urlParts[2] // Since 'svc' is at position 1, environment is expected at position 2

	// Initialize the Pinecone client with the extracted details
	client, err := pinecone.NewIndexClient(
		pinecone.WithIndexName(indexName),
		pinecone.WithEnvironment(environment),
		pinecone.WithProjectName(projectName),
		pinecone.WithAPIKey(d.config.PineconeAPIKey), // Use your actual API key
	)
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successfully created a Pinecone Client...")
	return client, nil
}
