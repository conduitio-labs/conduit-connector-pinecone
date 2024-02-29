package destination

import (
	"context"
	pinecone "github.com/conduitio-labs/conduit-connector-pinecone/destination"
	"testing"

	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := pinecone.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}a
