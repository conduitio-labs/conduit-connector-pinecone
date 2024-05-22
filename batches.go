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
func buildBatches(records []sdk.Record) ([]recordBatch, error) {
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
			id := recordID(rec.Key)
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

type recordBatch interface {
	writeBatch(context.Context, *pinecone.IndexConnection) (int, error)
}

type upsertBatch struct {
	vectors []*pinecone.Vector
}

func (b upsertBatch) writeBatch(ctx context.Context, index *pinecone.IndexConnection) (int, error) {
	written, err := index.UpsertVectors(&ctx, b.vectors)
	if err != nil {
		return 0, fmt.Errorf("failed to upsert vectors: %w", err)
	}
	return int(written), err
}

type deleteBatch struct {
	ids []string
}

func (b deleteBatch) writeBatch(ctx context.Context, index *pinecone.IndexConnection) (int, error) {
	err := index.DeleteVectorsById(&ctx, b.ids)
	if err != nil {
		return 0, fmt.Errorf("failed to delete vectors: %w", err)
	}

	return len(b.ids), nil
}
