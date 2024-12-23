package outbox_test

import (
	"context"
	"outbox"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"outbox/mocks"
)

func TestForwarder_Forward(t *testing.T) {
	// Create mocks
	readerMock := new(mocks.Reader)
	publisherMock := new(mocks.Publisher)

	// Create the forwarder
	forwarder, err := outbox.NewForwarder(readerMock, publisherMock)
	assert.NoError(t, err)

	// Define test data
	ctx := context.Background()
	filter := outbox.MessageFilter{}
	limit := 10
	messages := []outbox.Message{
		outbox.FakeMessage(),
		outbox.FakeMessage(),
	}

	// Set up expectations
	readerMock.On("Read", ctx, filter, limit).Return(messages, nil)
	for _, message := range messages {
		publisherMock.On("Publish", ctx, message).Return(nil)
	}
	readerMock.On("Mark", ctx, mock.Anything).Return(int64(len(messages)), nil)

	// Call the method
	err = forwarder.Forward(ctx, filter, limit)
	assert.NoError(t, err)

	// Assert expectations
	readerMock.AssertExpectations(t)
	publisherMock.AssertExpectations(t)
}
