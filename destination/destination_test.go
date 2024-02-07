package destination

import (
	"context"
	connectorname "github.com/conduitio-labs/conduit-connector-pinecone/destination"
	"testing"

	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := connectorname.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
