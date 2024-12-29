package outbox

import "github.com/nikolayk812/pgx-outbox/types"

type WriteOption func(*writer)

func WithDisablePreparedBatch() WriteOption {
	return func(w *writer) {
		w.usePreparedBatch = false
	}
}

type ReadOption func(*reader)

func WithReadFilter(filter types.MessageFilter) ReadOption {
	return func(r *reader) {
		r.filter = filter
	}
}

type ForwardOption func(forwarder *forwarder)

func WithForwardFilter(filter types.MessageFilter) ForwardOption {
	return func(f *forwarder) {
		f.filter = filter
	}
}
