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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pinecone-io/go-pinecone/pinecone"
)

// buildBatches processes a slice of records and groups them into batches based
// on their operation type. New batches are started whenever the operation type
// switches from upsert to delete or vice versa.
// Records are batched this way so that we preserve conduit's requirement of writing
// records sequentially.
func buildBatches(records []sdk.Record, multicollection bool) ([]recordBatch, error) {
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

		if multicollection {
			if prevBatch.getNamespace() != namespace {
				return addNewBatch(rec, namespace)
			}
		}

		if prevBatch.isCompatible(rec) {
			return prevBatch.addRecord(rec)
		}
		return addNewBatch(rec, namespace)
	}

	for _, rec := range records {
		namespace, err := rec.Metadata.GetCollection()
		if err != nil {
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

type recordBatch interface {
	getNamespace() string
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
