package writer

import (
	"context"
	"fmt"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nekomeowww/go-pinecone"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	client  pinecone.IndexClient
	payload sdk.Record
}

// New creates new instance of the Writer.
func New(ctx context.Context, client *pinecone.IndexClient, payload sdk.Record) (*Writer, error) {

	writer := &Writer{
		client:  *client,
		payload: payload,
	}

	tableInfo, err := columntypes.GetTableInfo(ctx, writer.db, writer.table)
	if err != nil {
		return nil, fmt.Errorf("get table info: %w", err)
	}

	writer.columnTypes = tableInfo.ColumnTypes

	return writer, nil
}

// Insert row to sql server db.
func (w *Writer) Upsert(ctx context.Context, record sdk.Record) error {

	upsertParams := pinecone.UpsertVectorsParams{
		Vectors: []*pinecone.Vector{
			{
				ID:       "YOUR_VECTOR_ID",
				Values:   values,
				Metadata: map[string]any{"foo": "bar"},
			},
		},
	}

	UpsertResponse, err := client.UpsertVectors(ctx, upsertParams)
	if err != nil {
		fmt.Print(err)
	}
	fmt.Print(UpsertResponse)

	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	keys, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	if len(keys) == 0 {
		return ErrNoKey
	}

	query, args := w.buildDeleteQuery(tableName, keys)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}
