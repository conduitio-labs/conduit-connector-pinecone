# Conduit Connector for Pinecone

The Pinecone connector is one of [Conduit](https://github.com/ConduitIO/conduit) builtin plugins. It provides both, a source and a destination connector for [Pinecone](https://www.Pinecone.io/).

It uses the [grpc go Pinecone client](github.com/Pinecone-io/go-Pinecone) to connect to Pinecone.

## What data does the OpenCDC record consist of?

| Field                   | Description                                                                             |
|-------------------------|-----------------------------------------------------------------------------------------|
| `record.Operation`      | which conduit operation does the record do.                                             |
| `record.Metadata`       | a string to string map representing the Pinecone vector metadata.                       |
| `record.Key`            | the vector id that is being updated.                                                    |
| `record.Payload.Before` | <empty>                                                                                 |
| `record.Payload.After`  | the vector body, in json format             | 
| `(json) record.Payload.After.values`  | an array of float32 representing the vector values              | 
| `(json) record.Payload.After.sparse_values`  | **(optional)** the sparse vector values               | 
| `(json) record.Payload.After.sparse_values.indices`  | an array of uint32 representing the sparse vector indices              | 
| `(json) record.Payload.After.sparse_values.values`  | an array of float32 representing the sparse vector values               | 

## How to Build?

Run `make build` to compile the connector.

## Testing

To perform the tests locally you'll need the `API_KEY` and `HOST_URL` environment variables set. To do so:

1. Open the `.env.example` file and fill up the variables.
2. Rename `.env.example` to `.env`
3. Run `make test` to run all tests.

## Destination Configuration Parameters

| Name                   | Description                                                                 | Required | Default Value |
|------------------------|-----------------------------------------------------------------------------|----------|---------------|
| `apiKey`            | The Pinecone api key.                          | Yes      |               |
| `host`            | The Pinecone index host.                          | Yes      |               |
| `namespace`            | The Pinecone namespace to target. Defaults to the default Pinecone namespace                           | No      |               |

## Example pipeline configuration

[Here's](./pipeline.destination.yml) an example of a complete configuration pipeline for a Pinecone destination connector.