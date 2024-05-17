package pinecone

import (
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestRecordMetadata(t *testing.T) {
	is := is.New(t)

	sdkMetadata := sdk.Metadata{
		"created_at":     "2023-03-15T14:25:07Z",
		"prop1": "a1",
		"prop2": "a2",
	}
	recMetadata, err := recordMetadata(sdkMetadata)
	is.NoErr(err)

	is.Equal(recMetadata.Fields["prop1"].AsInterface().(string), "a1")
	is.Equal(recMetadata.Fields["prop2"].AsInterface().(string), "a2")
}
