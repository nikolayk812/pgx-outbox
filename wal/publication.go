package wal

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

func (r *Reader) createPublication(ctx context.Context) error {
	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert')", r.publication, r.table)

	result := r.getConn().Exec(ctx, query)
	defer closeResource("create_publication_query_result", result)

	_, err := result.ReadAll()
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "42P01" { // SQLSTATE for "relation does not exist"
				return ErrTableNotFound
			}
		}
		return fmt.Errorf("result.ReadAll: %w", err)
	}

	return nil
}

// it is not checking other fields: table name, pubinsert, pubupdate, pubdelete, etc.
func (r *Reader) publicationExists(ctx context.Context) (bool, error) {
	query := fmt.Sprintf("SELECT pubname FROM pg_publication WHERE pubname = '%s'", r.publication)

	result := r.getConn().Exec(ctx, query)
	defer closeResource("publication_exists_query_result", result)

	row, err := toRow(result)
	if err != nil {
		return false, fmt.Errorf("toRow: %w", err)
	}

	if len(row) == 0 {
		return false, nil
	}

	return true, nil
}
