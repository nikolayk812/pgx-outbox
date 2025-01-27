package wal

import "time"

type ReadOption func(*Reader)

func WithPermanentSlot() ReadOption {
	return func(r *Reader) {
		r.permanentSlot = true
	}
}

func WithStandbyTimeout(timeout time.Duration) ReadOption {
	return func(r *Reader) {
		r.standbyTimeout = timeout
	}
}

func WithChannelBuffer(buffer int) ReadOption {
	return func(r *Reader) {
		r.channelBuffer = buffer
	}
}
