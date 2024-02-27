package pinecone

import (
	"github.com/conduitio-labs/conduit-connector-pinecone/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Connector combines all constructors for each plugin in one struct.
var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        nil,
	NewDestination:   destination.NewDestination,
}
