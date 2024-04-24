package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/matryer/is"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

func destConfigFromEnv() DestinationConfig {
	return DestinationConfig{
		PineconeAPIKey: os.Getenv("API_KEY"),
		PineconeHost:   os.Getenv("HOST_URL"),
	}
}

func TestWriter(t *testing.T) {
	is := is.New(t)
	cfg := destConfigFromEnv()
	ctx := context.Background()

	writer, err := NewWriter(context.Background(), cfg)
	is.NoErr(err)

	id := uuid.NewString()
	position := sdk.Position(fmt.Sprintf("pos-%v", id))
	metadata := map[string]string{
		"pinecone.prop1": "val1",
		"pinecone.prop2": "val2",
	}

	vecsToBeWritten := recordPayload{
		Id:     id,
		Values: []float32{1, 2},
	}

	payload, err := json.Marshal(vecsToBeWritten)
	is.NoErr(err)

	rec := sdk.Util.Source.NewRecordCreate(position, metadata, sdk.RawData(id), sdk.RawData(payload))

	err = writer.Upsert(ctx, rec)
	is.NoErr(err)

	recs, err := writer.index.FetchVectors(&ctx, []string{id})
	is.NoErr(err)

	fmt.Println(recs.Vectors)
}

func TestDestination_Integration_WriteDelete(t *testing.T) {
	ctx := context.Background()
	destCfg := destConfigFromEnv()
	is := is.New(t)
	index := createIndex(is)

	dest := NewDestination()

	err := dest.Configure(ctx, destCfg.toMap())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	defer teardown(is, ctx, dest)

	id := uuid.NewString()
	position := sdk.Position(fmt.Sprintf("pos-%v", id))
	metadata := map[string]string{
		"pinecone.prop1": "val1",
		"pinecone.prop2": "val2",
	}

	vecsToBeWritten := recordPayload{
		Id:           id,
		Values:       []float32{1, 2},
		SparseValues: sparseValues{},
	}

	payload, err := json.Marshal(vecsToBeWritten)
	is.NoErr(err)

	rec := sdk.Util.Source.NewRecordCreate(position, metadata, sdk.RawData(id), sdk.RawData(payload))

	_, err = dest.Write(ctx, []sdk.Record{rec})
	is.NoErr(err)
	// defer deleteRecord(is, index, id)

	assertWrittenRecordIndex(is, ctx, index, id, vecsToBeWritten)

	rec = sdk.Util.Source.NewRecordDelete(position, metadata, sdk.RawData(id))
	_, err = dest.Write(ctx, []sdk.Record{rec})

	assertDeletedRecordIndex(is, ctx, index, id)
}

func createIndex(is *is.I) *pinecone.IndexConnection {
	destCfg := destConfigFromEnv()

	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: destCfg.PineconeAPIKey,
	})
	is.NoErr(err)

	index, err := client.Index(destCfg.PineconeHost)
	is.NoErr(err)

	// index.Namespace = "default"

	return index
}

func assertWrittenRecordClient(is *is.I, ctx context.Context, client *pineconeClient, id string, writtenVecs UpsertBody) {
	vectors, err := client.fetchVectors([]string{id})
	is.NoErr(err)

	vec, ok := vectors[id]
	if !ok {
		is.Fail() // vector not found
	}

	is.Equal(vec.Values, writtenVecs.Vectors[0].Values)
	// is.Equal(vec.SparseValues.Indices, recVecValues.SparseValues.Indices)
	// is.Equal(vec.SparseValues.Values, recVecValues.SparseValues.Values)
}

func assertWrittenRecordIndex(is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string, writtenVecs recordPayload) {
	res, err := index.FetchVectors(&ctx, []string{id})
	is.NoErr(err)

	vec, ok := res.Vectors[id]
	if !ok {
		is.Fail() // vector not found
	}

	is.Equal(vec.Values, writtenVecs.Values)
	// is.Equal(vec.SparseValues.Indices, recVecValues.SparseValues.Indices)
	// is.Equal(vec.SparseValues.Values, recVecValues.SparseValues.Values)
}

func assertDeletedRecordClient(is *is.I, ctx context.Context, client *pineconeClient, id string) {
	vectors, err := client.fetchVectors([]string{id})
	is.NoErr(err)

	_, ok := vectors[id]
	if ok {
		is.Fail() // vector found, not properly deleted
	}
}

func assertDeletedRecordIndex(is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string) {
	res, err := index.FetchVectors(&ctx, []string{id})
	is.NoErr(err)

	_, ok := res.Vectors[id]
	if ok {
		is.Fail() // vector found, not properly deleted
	}
}

func deleteRecord(is *is.I, index *pinecone.IndexConnection, id string) {
	ctx := context.Background()
	err := index.DeleteVectorsById(&ctx, []string{id})
	is.NoErr(err)
}

func TestMain(t *testing.M) {
	if err := godotenv.Load(); err != nil {
		log.Fatal("error loading environment variables")
	}

	t.Run()
}

type connectorResource interface {
	Teardown(ctx context.Context) error
}

func teardown(is *is.I, ctx context.Context, resource connectorResource) {
	err := resource.Teardown(ctx)
	is.NoErr(err)
}
