# Conduit destination connector for Pinecone

The Pinecone connector is one of [Conduit](https://github.com/ConduitIO/conduit) standalone plugins. It provides a destination connector for [Pinecone](https://www.Pinecone.io/).

It uses the [gRPC go Pinecone client](github.com/Pinecone-io/go-Pinecone) to connect to Pinecone.

## How is the pinecone vector written?

Upsert and delete operations are batched while preserving Conduit's write order guarantee.

| Field                   | Description                                                                                                                                     |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `record.Operation`      | create, update and snapshot ops will be considered as vector upsert operations. Delete op will delete the vector using the record key.                                                                                                       |
| `record.Metadata`       | represents the pinecone vector metadata. All the record metadata is written as-is to it.                        |
| `record.Key`            | represents the vector id.                                                                                                           |
| `record.Payload.Before` | discarded, won't be used.                                                                                                                                     |
| `record.Payload.After`  | the vector body, in json format. Ignored in the delete op                                                                                                                 | 

## What OpenCDC data format does the destination connector accept?

The destination connector expects the `record.Payload.After` to be JSON formatted as follows:

| Field                   | Description                                                                                                                                     |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `record.Payload.After`  | both RawData (with json inside) and StructuredData conduit types are accepted. However, note that if the underlying type is StructuredData it will be marshaled and unmarshaled back redundantly, unlike RawData.               | 
| `record.Payload.After.values`  | an array of float32 representing the vector values              | 
| `record.Payload.After.sparse_values`  | **(optional)** the sparse vector values               | 
| `record.Payload.After.sparse_values.indices`  | an array of uint32 representing the sparse vector indices              | 
| `record.Payload.After.sparse_values.values`  | an array of float32 representing the sparse vector values               | 

## How to Build?

Run `make build` to compile the connector.

## Testing

To perform the tests locally you'll need the `API_KEY` and `HOST_URL` environment variables set. To do so:

1. You'll need to setup a new account if you don't have it at https://www.pinecone.io/   
2. Create a new index with `cosine` as the metric, and copy the host url.
3. Create a new api key.
4. Open the `.env.example` file and fill up the variables.
5. Rename `.env.example` to `.env`
6. Finally run `make test` to run all tests.                       

## Destination Configuration Parameters

| Name                   | Description                                                                 | Required | Default Value |
|------------------------|-----------------------------------------------------------------------------|----------|---------------|
| `apiKey`            | The Pinecone api key.                          | Yes      |               |
| `host`            | The Pinecone index host.                          | Yes      |               |
| `namespace`            | The Pinecone namespace to target. Defaults to the default Pinecone namespace                           | No      |               |

## Example pipeline configuration

[Here's](./pipeline.destination.yml) an example of a complete configuration pipeline for the Pinecone destination connector.
