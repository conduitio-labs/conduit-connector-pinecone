package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

type pineconeClient struct {
	host   string
	apikey string
	client *http.Client
}

func newPineconeClient(config DestinationConfig) *pineconeClient {
	return &pineconeClient{
		host:   config.PineconeHost,
		apikey: config.PineconeAPIKey,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (pc pineconeClient) newRequest(method, path string, body any) (*http.Request, error) {
	bs, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(bs)
	url := fmt.Sprintf("https://%s%s", pc.host, path)
	fmt.Println("url", url)
	fmt.Println("buf", buf.String())
	req, err := http.NewRequest(method, url, buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Api-Key", pc.apikey)
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func (pc pineconeClient) newGet(path string) (*http.Request, error) {
	url := fmt.Sprintf("https://%s%s", pc.host, path)
	fmt.Println("url", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Api-Key", pc.apikey)

	return req, nil
}

type Vector struct {
	Id     string    `json:"id"`
	Values []float32 `json:"values"`
}

type UpsertBody struct {
	Namespace string   `json:"namespace"`
	Vectors   []Vector `json:"vectors"`
}

func (pc pineconeClient) Upsert(ctx context.Context, rec sdk.Record) error {
	var body UpsertBody
	err := json.Unmarshal(rec.Payload.After.Bytes(), &body)
	if err != nil {
		return err
	}

	req, err := pc.newRequest("POST", "/vectors/upsert", body)
	if err != nil {
		return err
	}

	res, err := pc.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		bs, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("bad status code %v, payload %s", res.StatusCode, string(bs))
	}

	return nil
}

type DeleteBody struct {
	Ids       []string `json:"ids"`
	Namespace string   `json:"namespace,omitempty"`
}

func (pc pineconeClient) Delete(ctx context.Context, rec sdk.Record) error {
	payload, err := parseRecordPayload(rec.Payload)
	if err != nil {
		return err
	}

	body := DeleteBody{
		Ids: []string{payload.Id},
	}

	req, err := pc.newRequest("POST", "/vectors/delete", body)
	if err != nil {
		return err
	}

	res, err := pc.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		bs, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("bad status code %v, payload %s", res.StatusCode, string(bs))
	}

	return nil
}

func (pc pineconeClient) fetchVectors(ids []string) (map[string]Vector, error) {
	query := make(url.Values)
	idsVal := strings.Join(ids, ",")
	query.Set("ids", idsVal)

	req, err := pc.newGet("/vectors/fetch?" + query.Encode())
	if err != nil {
		return nil, err
	}

	res, err := pc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	bs, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	fmt.Println("payload", string(bs))

	var payload struct {
		Vectors map[string]Vector `json:"vectors"`
	}

	if err := json.Unmarshal(bs, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	return payload.Vectors, nil
}

func (pc pineconeClient) deleteVector(ids []string) error {
	// how can I refactor this code so that I don't have to use io.Readall? I think there's some io utilities that I can use so that I don't have to use io.ReadAll
	req, err := pc.newRequest("POST", "/vectors/delete", map[string]any{
		"ids": ids,
	})
	if err != nil {
		return err
	}

	res, err := pc.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		bs, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("bad status code %v, payload %s", res.StatusCode, string(bs))
	}

	return nil
}

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client *pinecone.Client
	index  *pinecone.IndexConnection
}

func NewWriter(ctx context.Context, config DestinationConfig) (*Writer, error) {
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: config.PineconeAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Pinecone client: %v", err)
	}
	sdk.Logger(ctx).Info().Msg("created pinecone client")

	// index urls should have their protocol trimmed
	host := strings.TrimPrefix(config.PineconeHost, "https://")

	index, err := client.Index(host)
	if err != nil {
		return nil, fmt.Errorf("error establishing index connection: %v", err)
	}
	sdk.Logger(ctx).Info().Msgf("created pinecone index")

	writer := &Writer{
		client: client,
		index:  index,
	}
	return writer, nil
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
		Id:     id,
		Values: payload.Values,
		// SparseValues: payload.PineconeSparseValues(),
		Metadata: metadata,
	}

	_, err = w.index.UpsertVectors(&ctx, []*pinecone.Vector{vec})
	if err != nil {
		return fmt.Errorf("error upserting record: %v ", err)
	}

	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	ids := []string{recordID(record.Key)}

	err := w.index.DeleteVectorsById(&ctx, ids)
	if err != nil {
		return fmt.Errorf("error deleting record: %v", err)
	}

	sdk.Logger(ctx).Trace().Msgf("deleted record %v", ids)
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
