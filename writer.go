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

func NewWriter(ctx context.Context, config DestinationConfig) (*Writer, error) {
	var w Writer
	var err error
	w.client, err = pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.APIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	// index urls should have their protocol trimmed
	host := strings.TrimPrefix(config.Host, "https://")

	if config.Namespace != "" {
		w.index, err = w.client.IndexWithNamespace(host, config.Namespace)
		if err != nil {
			return nil, fmt.Errorf(
				"error establishing index connection to namespace %v: %w",
				config.Namespace, err)
		}
	} else {
		w.index, err = w.client.Index(host)
		if err != nil {
			return nil, fmt.Errorf("error establishing index connection: %v", err)
		}
	}
	sdk.Logger(ctx).Info().Msg("created pinecone index")

	return &w, nil
}

func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {
	id := recordID(record.Key)

	payload, err := parseRecordPayload(record.Payload)
	if err != nil {
		return fmt.Errorf("error getting payload: %v", err)
	}

	metadata, err := recordMetadata(record.Metadata)
	if err != nil {
		return fmt.Errorf("error getting metadata: %v", err)
	}

	vec := &pinecone.Vector{
		Id:           id,
		Values:       payload.Values,
		SparseValues: payload.PineconeSparseValues(),
		Metadata:     metadata,
	}

	_, err = w.index.UpsertVectors(&ctx, []*pinecone.Vector{vec})
	if err != nil {
		return fmt.Errorf("error upserting record: %v ", err)
	}
	sdk.Logger(ctx).Trace().Msgf("upserted record id %s", id)

	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	id := recordID(record.Key)
	ids := []string{id}

	err := w.index.DeleteVectorsById(&ctx, ids)
	if err != nil {
		return fmt.Errorf("error deleting record: %v", err)
	}
	sdk.Logger(ctx).Trace().Msgf("deleted record %v", id)

	return nil
}

func (w *Writer) Close() error {
	return w.index.Close()
}

func recordID(key sdk.Data) string {
	return string(key.Bytes())
}

type sparseValues struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

type recordPayload struct {
	Id           string       `json:"id"`
	Values       []float32    `json:"values"`
	SparseValues sparseValues `json:"sparse_values,omitempty"`
	Namespace    string       `json:"namespace"`
}

func (r recordPayload) PineconeSparseValues() *pinecone.SparseValues {
	// the used pinecone go client needs a nil pointer when no sparse values given, or else it
	// will throw a "Sparse vector must contain at least one value" error
	if len(r.SparseValues.Indices) == 0 && len(r.SparseValues.Values) == 0 {
		return nil
	}

	v := &pinecone.SparseValues{r.SparseValues.Indices, r.SparseValues.Values}
	return v
}

func parseRecordPayload(payload sdk.Change) (parsed recordPayload, err error) {
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

func recordMetadata(data sdk.Metadata) (*pinecone.Metadata, error) {
	convertedMap := make(map[string]any)
	for key, value := range data {
		if trimmed, hasPrefix := trimPineconeKey(key); hasPrefix {
			convertedMap[trimmed] = value
		}
	}
	metadata, err := structpb.NewStruct(convertedMap)
	if err != nil {
		return nil, fmt.Errorf("error creating metadata: %v", err)
	}

	return metadata, nil
}

var keyPrefix = "pinecone."

func trimPineconeKey(key string) (trimmed string, hasPrefix bool) {
	if strings.HasPrefix(key, keyPrefix) {
		return key[len(keyPrefix):], true
	}

	return key, false
}
