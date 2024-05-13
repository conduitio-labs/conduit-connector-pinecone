package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/matryer/is"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

func TestDestination_Integration_Insert(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	dest := NewDestination()

	destCfg := destConfigFromEnv()

	err := dest.Configure(ctx, destCfg.toMap())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	defer teardown(is, ctx, dest)

	id := uuid.NewString()
	payload, err := json.Marshal([]float32{1, 1})
	is.NoErr(err)

	metadata := map[string]string{
		"prop1": "val1",
		"prop2": "val2",
	}

	rec := sdk.Util.Source.NewRecordCreate(
		sdk.Position(fmt.Sprintf("pos-%v", id)),
		metadata,
		sdk.RawData(id),
		sdk.RawData(payload),
	)

	_, err = dest.Write(ctx, []sdk.Record{rec})
	is.NoErr(err)

	index := createIndex(is)
	assertWrittenRecord(is, ctx, index, id, rec)
}

func destConfigFromEnv() DestinationConfig {
	return DestinationConfig{
		PineconeAPIKey:  os.Getenv("API_KEY"),
		PineconeHostURL: os.Getenv("HOST_URL"),
	}
}

func createIndex(is *is.I) *pinecone.IndexConnection {
	destCfg := destConfigFromEnv()

	client, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: destCfg.PineconeAPIKey,
	})
	is.NoErr(err)

	index, err := client.Index(destCfg.PineconeHostURL)
	is.NoErr(err)

	return index
}

func assertWrittenRecord(is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string, rec sdk.Record) {
	res, err := index.FetchVectors(&ctx, []string{id})
	is.NoErr(err)

	vec, ok := res.Vectors[id]
	if !ok {
		is.Fail() // vector not found
	}

	recVecValues, err := parseVectorValues(rec.Payload)
	is.NoErr(err)

	is.Equal(vec.Values, recVecValues)
}

func TestMain(t *testing.M) {
	_ = godotenv.Load()
	t.Run()
}

func teardown(is *is.I, ctx context.Context, destination sdk.Destination) {
	err := destination.Teardown(ctx)
	is.NoErr(err)
}
