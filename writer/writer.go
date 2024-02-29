package writer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nekomeowww/go-pinecone"
	"github.com/pkg/errors"
	"math"
	"strings"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.IndexClient
}

// NewWriter New creates new instance of the Writer.
func NewWriter(ctx context.Context) (*Writer, error) {

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

	payload, err := bytesToFloat32s(record.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("error converting data to []float32: %v", err)
	}

	record.Key.Un

	if err != nil {
		return fmt.Errorf("failed to unmarshal payload to structured data: %w", err)
	}

	upsertParams := pinecone.UpsertVectorsParams{
		Vectors: []*pinecone.Vector{
			{
				ID:       string(record.Key.Bytes()),
				Values:   payload,
				Metadata: record.Metadata,
			},
		},
	}

	UpsertResponse, err := client.UpsertVectors(ctx, upsertParams)
	if err != nil {
		fmt.Print(err)
	}

	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
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

func bytesToFloat32s(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("the byte slice length must be a multiple of 4")
	}

	var floats []float32
	for i := 0; i < len(data); i += 4 {
		bits := binary.BigEndian.Uint32(data[i : i+4])
		float := math.Float32frombits(bits)
		floats = append(floats, float)
	}

	return floats, nil
}

func recordMetadata(record sdk.Record) (map[string]interface{}, error) {
	data := record.Metadata

	if data == nil || len(data.Bytes()) == 0 {
		return nil, errors.New("empty payload")
	}

	properties := make(map[string]interface{})
	err := json.Unmarshal(data.Bytes(), &properties)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload to structured data: %w", err)
	}

	return properties, nil
}
