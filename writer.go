package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.Client
	index  *pinecone.IndexConnection
}

// NewWriter New creates new instance of the Writer.
func NewWriter(ctx context.Context, config DestinationConfig) (*Writer, error) {
	sdk.Logger(ctx).Trace().Msg("Creating new writer.")

	pineconeClient, indexConnection, err := NewPineconeClient(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pinecone pineconeClient: %v", err)
	}

	return &Writer{
		client: pineconeClient,
		index:  indexConnection,
	}, nil
}

func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {
	ID := recordID(record.Key)

	payload, err := parsePineconePayload(record.Payload)
	if err != nil {
		return fmt.Errorf("error getting payload: %v", err)
	}

	metadata, err := parseVectorMetadata(record.Metadata)
	if err != nil {
		return fmt.Errorf("error getting metadata: %v", err)
	}

	sdk.Logger(ctx).Error().Msgf("metadata: %v", metadata)

	vector := []*pinecone.Vector{{
		Id:     ID,
		Values: payload.Values,
		SparseValues: &pinecone.SparseValues{
			Indices: payload.SparseValues.Indices,
			Values:  payload.SparseValues.Values,
		},
		Metadata: metadata,
	}}

	_, err = w.index.UpsertVectors(&ctx, vector)
	if err != nil {
		return fmt.Errorf("error upserting record: %v ", err)
	}

	sdk.Logger(ctx).Trace().Msg("upserted record")
	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	ID := []string{recordID(record.Key)}

	err := w.index.DeleteVectorsById(&ctx, ID)
	if err != nil {
		return fmt.Errorf("\n error deleting record: %v ", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successful record delete.")
	return nil
}

func (w *Writer) Close() error {
	return w.index.Close()
}

// NewPineconeClient takes the Pinecone Index URL string in Config and splits into respective parts to establish a
// connection
func NewPineconeClient(ctx context.Context, config DestinationConfig) (*pinecone.Client, *pinecone.IndexConnection, error) {
	sdk.Logger(ctx).Trace().Msg("Creating a Pinecone Client.")
	fmt.Println(config.PineconeHostURL[:20])

	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.PineconeAPIKey,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	// index urls should have their protocol trimmed
	hostURL := strings.TrimPrefix(config.PineconeHostURL, "https://")

	index, err := client.Index(hostURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error establishing index connection: %v", err)
	}
	sdk.Logger(ctx).Info().Msgf("created pinecone index")

	return client, index, nil
}

func recordID(key sdk.Data) string {
	return string(key.Bytes())
}

type pineconePayload struct {
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values"`
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

func parsePineconePayload(payload sdk.Change) (parsed pineconePayload, err error) {
	data := payload.After

	if data == nil || len(data.Bytes()) == 0 {
		return parsed, errors.New("empty payload")
	}

	err = json.Unmarshal(data.Bytes(), &parsed)
	if err != nil {
		return parsed, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	return parsed, nil
}

func parseVectorMetadata(data sdk.Metadata) (*pinecone.Metadata, error) {
	convertedMap := make(map[string]interface{})
	for key, value := range data {
		convertedMap[key] = value
	}

	metadata, err := structpb.NewStruct(convertedMap)
	if err != nil {
		return nil, fmt.Errorf("error creating metadata: %v", err)
	}

	return metadata, nil
}
