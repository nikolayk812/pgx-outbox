package outbox

import "errors"

var (
	ErrTxNil             = errors.New("tx is nil")
	ErrTxUnsupportedType = errors.New("tx has unsupported type")

	ErrTableEmpty = errors.New("table is empty")
)
