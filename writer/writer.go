package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-pinecone/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/nekomeowww/go-pinecone"
	"github.com/pkg/errors"
	"strings"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.IndexClient
}

// NewWriter New creates new instance of the Writer.
func NewWriter(ctx context.Context, config config.DestinationConfig) (*Writer, error) {
	sdk.Logger(ctx).Trace().Msg("Creating new writer.")

	pineconeClient, err := NewPineconeClient(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pinecone pineconeClient: %v", err)
	}

	writer := &Writer{
		client: pineconeClient,
	}
	return writer, nil
}

func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {

	ID := recordID(record.Key)

	payload, err := recordPayload(record.Payload)
	if err != nil {
		fmt.Printf("\n error getting payload: %v", err)
	}

	metadata, err := recordMetadata(record.Metadata)
	if err != nil {
		fmt.Printf("\n error getting metadata: %v", err)
	}

	upsertParams := pinecone.UpsertVectorsParams{
		Vectors: []*pinecone.Vector{
			{
				ID:       ID,
				Values:   payload,
				Metadata: metadata,
			},
		},
	}

	fmt.Printf("ID: %v \nPayload: %v \nmetadata: %v\n", ID, len(payload), metadata)

	_, err = w.client.UpsertVectors(ctx, upsertParams)
	if err != nil {
		return fmt.Errorf("\n error upserting record: %v ", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successful record upsert.")
	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {

	ID := []string{recordID(record.Key)}

	deleteParams := pinecone.DeleteVectorsParams{
		IDs:       ID,
		Namespace: "",
		DeleteAll: false,
		Filter:    nil,
	}

	err := w.client.DeleteVectors(ctx, deleteParams)
	if err != nil {
		return fmt.Errorf("\n error deleting record: %v ", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successful record delete.")
	return nil
}

// NewPineconeClient takes the Pinecone Index URL string in Config and splits into respective parts to establish a
// connection
func NewPineconeClient(ctx context.Context, config config.DestinationConfig) (*pinecone.IndexClient, error) {
	sdk.Logger(ctx).Trace().Msg("Creating a Pinecone Client.")

	urlWithoutProtocol := strings.TrimPrefix(config.PineconeHostURL, "https://")
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
		pinecone.WithAPIKey(config.PineconeAPIKey), // Use your actual API key
	)
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successfully created a Pinecone Client...")
	return client, nil
}

func recordID(Key sdk.Data) string {
	key := Key.Bytes()
	return uuid.NewMD5(uuid.NameSpaceOID, key).String()
}

func recordPayload(payload sdk.Change) ([]float32, error) {

	data := payload.After

	if data == nil || len(data.Bytes()) == 0 {
		return nil, errors.New("empty payload")
	}

	var unmarshalledPayload []float32

	err := json.Unmarshal(data.Bytes(), &unmarshalledPayload)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	var floats []float32
	for _, value := range unmarshalledPayload {
		floats = append(floats, value)
	}
	return floats, nil
}

func recordMetadata(data sdk.Metadata) (map[string]any, error) {
	convertedMap := make(map[string]any, len(data))
	for key, value := range data {
		convertedMap[key] = value
	}
	return convertedMap, nil
}
