package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/matryer/is"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

func destConfigFromEnv() DestinationConfig {
	return DestinationConfig{
		APIKey: os.Getenv("API_KEY"),
		Host:   os.Getenv("HOST_URL"),
	}
}

func TestDestination_NamespaceSet(t *testing.T) {
	ctx := context.Background()
	destCfg := destConfigFromEnv()
	destCfg.Namespace = "test-namespace"

	is := is.New(t)
	dest := newDestination()

	err := dest.Configure(ctx, destCfg.toMap())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)
	defer teardown(is, ctx, dest)

	is.Equal(dest.writer.index.Namespace, "test-namespace")
}

func TestDestination_Integration_WriteDelete(t *testing.T) {
	ctx := context.Background()
	destCfg := destConfigFromEnv()
	is := is.New(t)
	dest := newDestination()

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
		Id:     id,
		Values: []float32{1, 2},
	}

	payload, err := json.Marshal(vecsToBeWritten)
	is.NoErr(err)

	rec := sdk.Util.Source.NewRecordCreate(position, metadata, sdk.RawData(id), sdk.RawData(payload))
	_, err = dest.Write(ctx, []sdk.Record{rec})
	is.NoErr(err)

	assertWrittenRecordIndex(t, is, ctx, dest.writer.index, id, vecsToBeWritten)

	rec = sdk.Util.Source.NewRecordDelete(position, metadata, sdk.RawData(id))
	_, err = dest.Write(ctx, []sdk.Record{rec})

	assertDeletedRecordIndex(t, is, ctx, dest.writer.index, id)

	deleteAllRecords(is, dest.writer.index)
}

const MAX_RETRIES = 3

func waitTime(i int) time.Duration {
	wait := math.Pow(2, float64(i))
	return time.Duration(wait) * time.Second
}

func assertWrittenRecordIndex(t *testing.T, is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string, writtenVecs recordPayload) {

	// Pinecone writes appear to be asynchronous. At the very least, in the current free tier serverless
	// configuration that I've tested, pinecone writes occurred slightly after the RPC call
	// returned data. Therefore, the following retry logic is needed to make tests more robust
	for i := 1; i <= MAX_RETRIES; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		vec, ok := res.Vectors[id]
		if !ok {
			if i == MAX_RETRIES {
				is.Fail() // vector was not written
			} else {
				wait := waitTime(i)
				t.Logf("retrying with wait of %v", wait)
				time.Sleep(wait)
				continue
			}
		}

		is.Equal(vec.Values, writtenVecs.Values)
		break
	}
}

func assertDeletedRecordIndex(t *testing.T, is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string) {
	// same as assertWrittenRecordIndex, we need the retry for robustness
	for i := 0; i <= MAX_RETRIES; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		_, ok := res.Vectors[id]
		if ok {
			if i == MAX_RETRIES {
				is.Fail() // vector found, not properly deleted
			} else {
				wait := waitTime(i)
				t.Logf("retrying with wait of %v", wait)
				time.Sleep(wait)
				continue
			}
		}
	}
}

func deleteAllRecords(is *is.I, index *pinecone.IndexConnection) {
	ctx := context.Background()
	err := index.DeleteAllVectorsInNamespace(&ctx)
	is.NoErr(err)
}

func TestMain(t *testing.M) {
	godotenv.Load()

	t.Run()
}

type connectorResource interface {
	Teardown(ctx context.Context) error
}

func teardown(is *is.I, ctx context.Context, resource connectorResource) {
	err := resource.Teardown(ctx)
	is.NoErr(err)
}
