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
func (d *Destination) buildBatches(records []sdk.Record) ([]recordBatch, error) {
	var batches []recordBatch

	var currUpsertBatch upsertBatch
	var currDeleteBatch deleteBatch

	for i, rec := range records {
		isLast := i == len(records)-1
		switch rec.Operation {
		case sdk.OperationCreate, sdk.OperationUpdate, sdk.OperationSnapshot:
			vec, err := parsePineconeVector(rec)
			if err != nil {
				return nil, err
			}

			currUpsertBatch.vectors = append(currUpsertBatch.vectors, vec)

			if isLast {
				batches = append(batches, currUpsertBatch)
			}
			if len(currDeleteBatch.ids) != 0 {
				batches = append(batches, currDeleteBatch)
				currDeleteBatch = deleteBatch{}
			}
		case sdk.OperationDelete:
			id := vectorID(rec.Key)
			currDeleteBatch.ids = append(currDeleteBatch.ids, id)

			if isLast {
				batches = append(batches, currDeleteBatch)
			}
			if len(currUpsertBatch.vectors) != 0 {
				batches = append(batches, currUpsertBatch)
				currUpsertBatch = upsertBatch{}
			}
		}
	}

	return batches, nil
}

func buildBatches2(records []sdk.Record) ([]recordBatch, error) {
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
		if prevBatch.isCompatible(rec) {
			if err := prevBatch.addRecord(rec); err != nil {
				return err
			}
		} else {
			if err := addNewBatch(rec, namespace); err != nil {
				return err
			}
		}

		return nil
	}

	for _, rec := range records {
		namespace, err := rec.Metadata.GetCollection()
		if err != nil {
			// namespace is default
		}

		if len(batches) == 0 {
			addNewBatch(rec, namespace)
		} else {
			addToPreviousBatch(rec, namespace)
		}
	}

	return batches, nil
}

type recordBatch interface {
	isCompatible(sdk.Record) bool
	addRecord(sdk.Record) error
	writeBatch(context.Context, *pinecone.IndexConnection) (int, error)
}

type upsertBatch struct {
	namespace string
	vectors   []*pinecone.Vector
}

func (b *upsertBatch) isCompatible(rec sdk.Record) bool {
	switch rec.Operation {
	case sdk.OperationCreate, sdk.OperationUpdate, sdk.OperationSnapshot:
		return true
	}

	return false
}

func (b *upsertBatch) addRecord(rec sdk.Record) error {
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

func (b *deleteBatch) isCompatible(rec sdk.Record) bool {
	return rec.Operation == sdk.OperationDelete
}

func (b *deleteBatch) addRecord(rec sdk.Record) error {

}

func (b *deleteBatch) writeBatch(ctx context.Context, index *pinecone.IndexConnection) (int, error) {
	err := index.DeleteVectorsById(&ctx, b.ids)
	if err != nil {
		return 0, fmt.Errorf("failed to delete vectors: %w", err)
	}

	return len(b.ids), nil
}
