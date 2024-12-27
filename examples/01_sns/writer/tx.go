package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (r *repo) beginTx(ctx context.Context) (pgx.Tx, func(err error) error, error) {
	emptyFunc := func(err error) error { return nil }

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, emptyFunc, fmt.Errorf("pool.Begin: %w", err)
	}
	if tx == nil {
		// probably never happen
		return nil, emptyFunc, fmt.Errorf("tx is nil")
	}

	commitFunc := func(execErr error) error {
		// from executing a query
		if execErr != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("tx.Rollback %v: %w", execErr, rbErr)
			}
			return execErr
		}

		if txErr := tx.Commit(ctx); txErr != nil {
			return fmt.Errorf("tx.Commit: %w", txErr)
		}

		return nil
	}

	return tx, commitFunc, nil
}
