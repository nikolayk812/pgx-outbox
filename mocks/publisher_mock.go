// Code generated by mockery v2.50.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	types "github.com/nikolayk812/pgx-outbox/types"
)

// Publisher is an autogenerated mock type for the Publisher type
type Publisher struct {
	mock.Mock
}

// Publish provides a mock function with given fields: ctx, message
func (_m *Publisher) Publish(ctx context.Context, message types.Message) error {
	ret := _m.Called(ctx, message)

	if len(ret) == 0 {
		panic("no return value specified for Publish")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, types.Message) error); ok {
		r0 = rf(ctx, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewPublisher creates a new instance of Publisher. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewPublisher(t interface {
	mock.TestingT
	Cleanup(func())
}) *Publisher {
	mock := &Publisher{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
