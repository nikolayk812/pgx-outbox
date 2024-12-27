package types

// Tx is a transaction interface to support both and pgx.Tx and *sql.Tx.
// As pgx.Tx and *sql.Tx do not have common method signatures, this is empty interface.
type Tx interface{}
