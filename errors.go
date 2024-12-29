package outbox

import "errors"

var (
	ErrTxNil = errors.New("tx is nil")

	ErrTableEmpty = errors.New("table is empty")
)
