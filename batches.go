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
	"errors"
	"fmt"
	"strings"
	"text/template"

	sdk "github.com/conduitio/conduit-connector-sdk"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

type recordBatch interface {
	getNamespace() string

	// isCompatible examines the given record and returns whether the
	// record can be added to the batch or not.
	isCompatible(sdk.Record) bool

	addRecord(sdk.Record) error
	writeBatch(context.Context, *pinecone.IndexConnection) (int, error)
}

type upsertBatch struct {
	namespace string
	vectors   []*pinecone.Vector
}

func (b *upsertBatch) getNamespace() string {
	return b.namespace
}

func (b *upsertBatch) isCompatible(rec sdk.Record) bool {
	switch rec.Operation {
	case sdk.OperationCreate, sdk.OperationUpdate, sdk.OperationSnapshot:
		return true
	}

	return false
}

func (b *upsertBatch) addRecord(rec sdk.Record) error {
	vec, err := parsePineconeVector(rec)
	if err != nil {
		return err
	}

	b.vectors = append(b.vectors, vec)
	return nil
}

func (b *upsertBatch) writeBatch(ctx context.Context, index *pinecone.IndexConnection) (int, error) {
	written, err := index.UpsertVectors(&ctx, b.vectors)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert vectors: %w", err)
	}
	return int(written), err
}

type deleteBatch struct {
	namespace string
	ids       []string
}

func (b *deleteBatch) getNamespace() string {
	return b.namespace
}

func (b *deleteBatch) isCompatible(rec sdk.Record) bool {
	return rec.Operation == sdk.OperationDelete
}

func (b *deleteBatch) addRecord(rec sdk.Record) error {
	id := vectorID(rec.Key)
	b.ids = append(b.ids, id)
	return nil
}

func (b *deleteBatch) writeBatch(ctx context.Context, index *pinecone.IndexConnection) (int, error) {
	err := index.DeleteVectorsById(&ctx, b.ids)
	if err != nil {
		return 0, fmt.Errorf("failed to delete vectors: %w", err)
	}

	return len(b.ids), nil
}

type collectionWriter interface {
	writeRecords(context.Context, []sdk.Record) (int, error)
	close() error
}

type multicollectionWriter struct {
	apiKey, host string

	indexes           cmap.ConcurrentMap[string, *pinecone.IndexConnection]
	namespaceTemplate *template.Template
}

func newMulticollectionWriter(apiKey, host string, template *template.Template) *multicollectionWriter {
	return &multicollectionWriter{
		apiKey:            apiKey,
		host:              host,
		indexes:           cmap.New[*pinecone.IndexConnection](),
		namespaceTemplate: template,
	}
}

const defaultNamespace = "(default)"

func (w *multicollectionWriter) parseNamespace(record sdk.Record) (string, error) {
	if w.namespaceTemplate != nil {
		var sb strings.Builder
		if err := w.namespaceTemplate.Execute(&sb, record); err != nil {
			return "", fmt.Errorf("failed to execute namespace template: %w", err)
		}

		return sb.String(), nil
	}

	namespace, err := record.Metadata.GetCollection()
	if err != nil {
		return defaultNamespace, nil
	}

	return namespace, nil
}

func (w *multicollectionWriter) addIndexIfMissing(ctx context.Context, namespace string) error {
	if w.indexes.Has(namespace) {
		return nil
	}

	if namespace == defaultNamespace {
		namespace = ""
	}

	index, err := newIndex(ctx, newIndexParams{
		apiKey:    w.apiKey,
		host:      w.host,
		namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("failed to create new index for namespace %s: %w", namespace, err)
	}

	sdk.Logger(ctx).Info().Str("namespace", namespace).Msg("connected to new namespaced index")

	w.indexes.Set(namespace, index)
	return nil
}

func (w *multicollectionWriter) buildBatches(ctx context.Context, records []sdk.Record) ([]recordBatch, error) {
	var batches []recordBatch

	addNewBatch := func(rec sdk.Record, namespace string) error {
		var batch recordBatch

		if rec.Operation == sdk.OperationDelete {
			batch = &deleteBatch{namespace: namespace}
		} else {
			batch = &upsertBatch{namespace: namespace}
		}

		if err := batch.addRecord(rec); err != nil {
			return err
		}

		batches = append(batches, batch)
		return nil
	}

	addToPreviousBatch := func(rec sdk.Record, namespace string) error {
		prevBatch := batches[len(batches)-1]

		if prevBatch.getNamespace() != namespace {
			return addNewBatch(rec, namespace)
		}

		if prevBatch.isCompatible(rec) {
			return prevBatch.addRecord(rec)
		}
		return addNewBatch(rec, namespace)
	}

	for _, rec := range records {
		namespace, err := w.parseNamespace(rec)
		if err != nil {
			return nil, fmt.Errorf("failed to parse namespace: %w", err)
		}

		// Note: we could paralelize the index creation, but is it worth it?
		if err := w.addIndexIfMissing(ctx, namespace); err != nil {
			return nil, fmt.Errorf("failed to add missing index: %w", err)
		}

		if len(batches) == 0 {
			err = addNewBatch(rec, namespace)
		} else {
			err = addToPreviousBatch(rec, namespace)
		}
		if err != nil {
			return nil, err
		}
	}

	return batches, nil
}

func (w *multicollectionWriter) writeRecords(ctx context.Context, records []sdk.Record) (int, error) {
	batches, err := w.buildBatches(ctx, records)
	if err != nil {
		return 0, err
	}

	var written int
	for _, batch := range batches {
		namespace := batch.getNamespace()
		index, ok := w.indexes.Get(namespace)
		if !ok {
			// should be unreachable, something went wrong when building batches
			panic(fmt.Sprintf("index not found for namespace %s", namespace))
		}

		batchWrittenRecs, err := batch.writeBatch(ctx, index)
		written += batchWrittenRecs
		if err != nil {
			return written, fmt.Errorf("failed to write record batch: %w", err)
		}
	}

	return written, nil
}

func (w *multicollectionWriter) close() error {
	var err error
	for tuple := range w.indexes.IterBuffered() {
		err = errors.Join(err, tuple.Val.Close())
	}

	if err != nil {
		return fmt.Errorf("failed to close indexes: %w", err)
	}

	return nil
}

type singleCollectionWriter struct {
	index *pinecone.IndexConnection
}

func (w *singleCollectionWriter) buildBatches(ctx context.Context, records []sdk.Record) ([]recordBatch, error) {
	var batches []recordBatch

	addNewBatch := func(rec sdk.Record) error {
		var batch recordBatch

		if rec.Operation == sdk.OperationDelete {
			batch = &deleteBatch{}
		} else {
			batch = &upsertBatch{}
		}

		if err := batch.addRecord(rec); err != nil {
			return err
		}

		batches = append(batches, batch)
		return nil
	}

	addToPreviousBatch := func(rec sdk.Record) error {
		prevBatch := batches[len(batches)-1]

		if prevBatch.isCompatible(rec) {
			return prevBatch.addRecord(rec)
		}
		return addNewBatch(rec)
	}

	for _, rec := range records {
		var err error
		if len(batches) == 0 {
			err = addNewBatch(rec)
		} else {
			err = addToPreviousBatch(rec)
		}
		if err != nil {
			return batches, err
		}
	}

	return batches, nil
}

func (w *singleCollectionWriter) writeRecords(ctx context.Context, records []sdk.Record) (int, error) {
	batches, err := w.buildBatches(ctx, records)
	if err != nil {
		return 0, err
	}

	var written int
	for _, batch := range batches {
		batchWrittenRecs, err := batch.writeBatch(ctx, w.index)
		written += batchWrittenRecs
		if err != nil {
			return written, fmt.Errorf("failed to write record batch: %w", err)
		}
	}

	return written, nil
}

func (w *singleCollectionWriter) close() error {
	return w.index.Close()
}
