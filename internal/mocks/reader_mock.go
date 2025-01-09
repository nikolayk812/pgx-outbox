// Code generated by mockery v2.50.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	types "github.com/nikolayk812/pgx-outbox/types"
)

// Reader is an autogenerated mock type for the Reader type
type Reader struct {
	mock.Mock
}

// Ack provides a mock function with given fields: ctx, ids
func (_m *Reader) Ack(ctx context.Context, ids []int64) ([]int64, error) {
	ret := _m.Called(ctx, ids)

	if len(ret) == 0 {
		panic("no return value specified for Ack")
	}

	var r0 []int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []int64) ([]int64, error)); ok {
		return rf(ctx, ids)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []int64) []int64); ok {
		r0 = rf(ctx, ids)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int64)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []int64) error); ok {
		r1 = rf(ctx, ids)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Read provides a mock function with given fields: ctx, limit
func (_m *Reader) Read(ctx context.Context, limit int) ([]types.Message, error) {
	ret := _m.Called(ctx, limit)

	if len(ret) == 0 {
		panic("no return value specified for Read")
	}

	var r0 []types.Message
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int) ([]types.Message, error)); ok {
		return rf(ctx, limit)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int) []types.Message); ok {
		r0 = rf(ctx, limit)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]types.Message)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int) error); ok {
		r1 = rf(ctx, limit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewReader creates a new instance of Reader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *Reader {
	mock := &Reader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
