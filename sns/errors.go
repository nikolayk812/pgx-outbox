package sns

import "errors"

var (
	ErrSnsClientNil   = errors.New("sns client is nil")
	ErrTransformerNil = errors.New("transformer is nil")
)
