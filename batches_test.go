// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pinecone

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"text/template"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

func assertUpsertBatch(is *is.I, batch recordBatch, records []opencdc.Record) {
	upsertBatch, ok := batch.(*upsertBatch)
	is.True(ok) // batch isn't upsertBatch

	for i, vec := range upsertBatch.vectors {
		rec := records[i]

		parsed, err := parsePineconeVector(rec)
		is.NoErr(err)

		is.Equal(vec, parsed)
	}
}

func assertDeleteBatch(is *is.I, batch recordBatch, records []opencdc.Record) {
	deleteBatch, ok := batch.(*deleteBatch)
	is.True(ok) // batch isn't deleteBatch

	keys := make([]string, len(records))
	for i, rec := range records {
		keys[i] = string(rec.Key.Bytes())
	}

	is.Equal(deleteBatch.ids, keys)
}

func TestSingleCollectionWriter(t *testing.T) {
	colWriter := singleCollectionWriter{}

	t.Run("empty", func(t *testing.T) {
		is := is.New(t)
		var records []opencdc.Record
		batches, err := colWriter.buildBatches(records)
		is.NoErr(err)

		is.Equal(len(batches), 0)
	})

	t.Run("only delete", func(t *testing.T) {
		is := is.New(t)
		records := testRecords(opencdc.OperationDelete)
		batches, err := colWriter.buildBatches(records)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertDeleteBatch(is, batches[0], records)
	})

	t.Run("only non delete", func(t *testing.T) {
		is := is.New(t)
		records := testRecords(opencdc.OperationCreate)
		batches, err := colWriter.buildBatches(records)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertUpsertBatch(is, batches[0], records)
	})

	t.Run("multiple ops", func(t *testing.T) {
		is := is.New(t)
		var records []opencdc.Record
		batch0 := testRecords(opencdc.OperationUpdate)
		records = append(records, batch0...)

		batch1 := testRecords(opencdc.OperationDelete)
		records = append(records, batch1...)

		batch2 := testRecords(opencdc.OperationCreate)
		records = append(records, batch2...)

		batch3 := testRecords(opencdc.OperationDelete)
		records = append(records, batch3...)

		batch4 := testRecords(opencdc.OperationSnapshot)
		records = append(records, batch4...)

		batches, err := colWriter.buildBatches(records)
		is.NoErr(err)

		is.Equal(len(batches), 5)

		assertUpsertBatch(is, batches[0], batch0)
		assertDeleteBatch(is, batches[1], batch1)
		assertUpsertBatch(is, batches[2], batch2)
		assertDeleteBatch(is, batches[3], batch3)
		assertUpsertBatch(is, batches[4], batch4)
	})
}

func setupMulticollection(t *testing.T) (context.Context, *is.I, *multicollectionWriter) {
	cfg := destConfigFromEnv(t)

	colWriter := newMulticollectionWriter(cfg.APIKey, cfg.Host, nil)
	ctx := context.Background()
	is := is.New(t)

	return ctx, is, colWriter
}

func TestParseNamespace(t *testing.T) {
	t.Run("from template", func(t *testing.T) {
		_, is, colWriter := setupMulticollection(t)

		colWriter.namespaceTemplate = template.Must(template.
			New("test").
			Parse(`{{ printf "%s" .Key }}`))

		namespace, err := colWriter.parseNamespace(opencdc.Record{
			Key: opencdc.RawData("testtemplate"),
		})
		is.NoErr(err)

		is.Equal(namespace, "testtemplate")
	})
	t.Run("from opencdc.collection", func(t *testing.T) {
		_, is, colWriter := setupMulticollection(t)

		namespace, err := colWriter.parseNamespace(opencdc.Record{
			Metadata: opencdc.Metadata{"opencdc.collection": "testtemplate"},
		})
		is.NoErr(err)

		is.Equal(namespace, "testtemplate")
	})
}

func TestMulticollectionWriter_buildBatches(t *testing.T) {
	t.Run("connects to multiple namespaces when building batches", func(t *testing.T) {
		ctx, is, colWriter := setupMulticollection(t)

		var recs []opencdc.Record
		recs1 := testRecordsWithNamespace(opencdc.OperationCreate, "namespace1")
		recs = append(recs, recs1...)
		recs2 := testRecordsWithNamespace(opencdc.OperationCreate, "namespace2")
		recs = append(recs, recs2...)
		recs3 := testRecordsWithNamespace(opencdc.OperationCreate, "namespace3")
		recs = append(recs, recs3...)

		_, err := colWriter.buildBatches(ctx, recs)
		is.NoErr(err)

		is.Equal(colWriter.indexes.Count(), 3)
	})

	t.Run("empty", func(t *testing.T) {
		ctx, is, colWriter := setupMulticollection(t)

		var records []opencdc.Record
		batches, err := colWriter.buildBatches(ctx, records)
		is.NoErr(err)

		is.Equal(len(batches), 0)
	})

	t.Run("only delete", func(t *testing.T) {
		ctx, is, colWriter := setupMulticollection(t)

		records := testRecords(opencdc.OperationDelete)
		batches, err := colWriter.buildBatches(ctx, records)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertDeleteBatch(is, batches[0], records)
	})

	t.Run("only non delete", func(t *testing.T) {
		ctx, is, colWriter := setupMulticollection(t)

		records := testRecords(opencdc.OperationCreate)
		batches, err := colWriter.buildBatches(ctx, records)
		is.NoErr(err)

		is.Equal(len(batches), 1)
		assertUpsertBatch(is, batches[0], records)
	})

	t.Run("multiple ops", func(t *testing.T) {
		ctx, is, colWriter := setupMulticollection(t)

		var records []opencdc.Record
		batch0 := testRecords(opencdc.OperationUpdate)
		records = append(records, batch0...)

		batch1 := testRecords(opencdc.OperationDelete)
		records = append(records, batch1...)

		batch2 := testRecords(opencdc.OperationCreate)
		records = append(records, batch2...)

		batch3 := testRecords(opencdc.OperationDelete)
		records = append(records, batch3...)

		batch4 := testRecords(opencdc.OperationSnapshot)
		records = append(records, batch4...)

		batches, err := colWriter.buildBatches(ctx, records)
		is.NoErr(err)

		is.Equal(len(batches), 5)

		assertUpsertBatch(is, batches[0], batch0)
		assertDeleteBatch(is, batches[1], batch1)
		assertUpsertBatch(is, batches[2], batch2)
		assertDeleteBatch(is, batches[3], batch3)
		assertUpsertBatch(is, batches[4], batch4)
	})
}

func TestMulticollectionWriter_WriteToMultipleNamespaces(t *testing.T) {
	ctx, is, colWriter := setupMulticollection(t)

	var recs []opencdc.Record
	recs1 := testRecordsWithNamespace(opencdc.OperationCreate, "namespace1")
	recs = append(recs, recs1...)
	recs2 := testRecordsWithNamespace(opencdc.OperationUpdate, "namespace2")
	recs = append(recs, recs2...)
	recs3 := testRecordsWithNamespace(opencdc.OperationCreate, "namespace3")
	recs = append(recs, recs3...)

	written, err := colWriter.writeRecords(ctx, recs)
	is.NoErr(err)

	is.Equal(written, len(recs1)+len(recs2)+len(recs3))

	index1 := assertUpsertRecordsWrittenInNamespace(ctx, t, is, recs1, "namespace1")
	deleteAllRecords(is, index1)

	index2 := assertUpsertRecordsWrittenInNamespace(ctx, t, is, recs2, "namespace2")
	deleteAllRecords(is, index2)

	index3 := assertUpsertRecordsWrittenInNamespace(ctx, t, is, recs3, "namespace3")
	deleteAllRecords(is, index3)
}

func testRecordsWithNamespace(op opencdc.Operation, namespace string) []opencdc.Record {
	total := rand.Intn(3) + 1
	recs := make([]opencdc.Record, total)

	for i := range total {
		position := opencdc.Position(randString())
		key := opencdc.RawData(randString())
		metadata := opencdc.Metadata{
			randString(): randString(),
			randString(): randString(),
		}
		if namespace != "" {
			metadata.SetCollection(namespace)
		}

		vecValues := pineconeVectorValues{
			Values: []float32{1, 2},
			SparseValues: sparseValues{
				Indices: []uint32{3, 5},
				Values:  []float32{0.5, 0.3},
			},
		}
		bs, err := json.Marshal(vecValues)
		if err != nil {
			// should never happen
			panic(err)
		}

		payload := opencdc.RawData(bs)

		rec := opencdc.Record{
			Position: position, Operation: op,
			Metadata: metadata, Key: key,
			Payload: opencdc.Change{
				Before: nil, // discarded, the Pinecone destination connector doesn't use this field
				After:  payload,
			},
		}
		recs[i] = rec
	}

	return recs
}

func testRecords(op opencdc.Operation) []opencdc.Record {
	return testRecordsWithNamespace(op, "")
}

func randString() string { return uuid.NewString()[0:8] }

func assertUpsertRecordsWrittenInNamespace(
	ctx context.Context, t *testing.T, is *is.I,
	recs []opencdc.Record, namespace string,
) *pinecone.IndexConnection {
	destCfg := destConfigFromEnv(t)
	destCfg.Namespace = namespace

	index := createIndex(is, destCfg)

	var ids []string
	for _, rec := range recs {
		ids = append(ids, string(rec.Key.Bytes()))
	}

	res, err := index.FetchVectors(ctx, ids)
	is.NoErr(err)

	for id, vec := range res.Vectors {
		for _, rec := range recs {
			if id != string(rec.Key.Bytes()) {
				continue
			}

			parsedVec, err := parsePineconeVector(rec)
			is.NoErr(err)

			is.Equal(vec, parsedVec)
			break
		}
	}

	return index
}
