package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	pinecone "github.com/conduitio-labs/conduit-connector-pinecone"
)

func main() {
	sdk.Serve(pinecone.Connector)
}
