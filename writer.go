package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
	"strings"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.Client
	index  *pinecone.IndexConnection
}

// NewWriter New creates new instance of the Writer.
func NewWriter(ctx context.Context, config DestinationConfig) (*Writer, error) {
	sdk.Logger(ctx).Trace().Msg("Creating new writer.")

	sdk.Logger(ctx).Error().Msgf("API: %v, INDEX:%v", config.PineconeAPIKey, config.PineconeHostURL)

	pineconeClient, indexConnection, err := NewPineconeClient(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pinecone pineconeClient: %v", err)
	}

	writer := &Writer{
		client: pineconeClient,
		index:  indexConnection,
	}
	return writer, nil
}

func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {

	ID := recordID(record.Key)

	sdk.Logger(ctx).Error().Msgf("LE PAYLOAD: %v", record.Payload.After)

	//payload, err := recordPayload(record.Payload)
	//if err != nil {
	//	return fmt.Errorf("error getting payload: %v", err)
	//}

	metadata, err := recordMetadata(record.Metadata)
	if err != nil {
		return fmt.Errorf("error getting metadata: %v", err)
	}

	sdk.Logger(ctx).Error().Msgf("metadata: %v", metadata)

	vals := []float32{0.000571884, 2}

	vector := []*pinecone.Vector{{
		Id:           ID,
		Values:       vals,
		SparseValues: nil,
		Metadata:     nil,
	}}

	//sdk.Logger(ctx).Trace().Msgf("ID: %v Payload: %v metadata: %v\n", ID, len(payload), metadata)

	_, err = w.index.UpsertVectors(&ctx, vector)
	if err != nil {
		return fmt.Errorf("error upserting record: %v ", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successful record upsert.")
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

// NewPineconeClient takes the Pinecone Index URL string in Config and splits into respective parts to establish a
// connection
func NewPineconeClient(ctx context.Context, config DestinationConfig) (*pinecone.Client, *pinecone.IndexConnection, error) {
	sdk.Logger(ctx).Trace().Msg("Creating a Pinecone Client.")

	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.PineconeAPIKey,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}
	sdk.Logger(ctx).Trace().Msg("Successfully init...")

	hostURL := strings.TrimPrefix(config.PineconeHostURL, "https://")
	sdk.Logger(ctx).Trace().Msg("Successfully trim...")

	index, err := client.Index(hostURL)
	sdk.Logger(ctx).Trace().Msgf("doesnt reach")

	defer index.Close()

	if err != nil {
		return nil, nil, fmt.Errorf("error establishing index connection: %v", err)
	}

	sdk.Logger(ctx).Trace().Msg("Successfully created a Pinecone Client...")
	return client, index, nil
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

func recordMetadata(data sdk.Metadata) (*pinecone.Metadata, error) {
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
