package pinecone

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "pinecone",
		Summary: "A pinecone destination plugin for Conduit, written in Go.",
		Version: version,
		Author:  "Adam Haffar",
	}
}
