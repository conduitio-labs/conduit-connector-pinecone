package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
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
		PineconeAPIKey: os.Getenv("API_KEY"),
		PineconeHost:   os.Getenv("HOST_URL"),
	}
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

	assertWrittenRecordIndex(is, ctx, dest.writer.index, id, vecsToBeWritten)

	rec = sdk.Util.Source.NewRecordDelete(position, metadata, sdk.RawData(id))
	_, err = dest.Write(ctx, []sdk.Record{rec})

	assertDeletedRecordIndex(is, ctx, dest.writer.index, id)

	deleteAllRecords(is, dest.writer.index)
}

func assertWrittenRecordIndex(is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string, writtenVecs recordPayload) {
	for i := 0; i < 3; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		vec, ok := res.Vectors[id]
		if !ok {
			if i == 2 {
				is.Fail() // vector was not written
			} else {
				time.Sleep(time.Duration(i) * time.Second)
				continue
			}
		}

		is.Equal(vec.Values, writtenVecs.Values)
		break
	}
}

func assertDeletedRecordIndex(is *is.I, ctx context.Context, index *pinecone.IndexConnection, id string) {
	for i := 0; i < 3; i++ {
		res, err := index.FetchVectors(&ctx, []string{id})
		is.NoErr(err)

		_, ok := res.Vectors[id]
		if ok {
			if i == 2 {
				is.Fail() // vector found, not properly deleted
			} else {
				time.Sleep(time.Duration(i) * time.Second)
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
