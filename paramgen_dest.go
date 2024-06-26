// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package pinecone

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (DestinationConfig) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"apiKey": {
			Default:     "",
			Description: "apiKey is the API Key for authenticating with Pinecone.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"host": {
			Default:     "",
			Description: "host is the whole Pinecone index host URL.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"namespace": {
			Default:     "",
			Description: "namespace is the Pinecone's index namespace. Defaults to the empty namespace. It can contain a [Go template](https://pkg.go.dev/text/template) that will be executed for each record to determine the namespace.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
