package outbox_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/stretchr/testify/assert"
	"outbox"
	"outbox/mocks"
)

var ctx = context.Background()

func TestForwarder_Forward(t *testing.T) {
	msg1 := outbox.FakeMessage()
	msg1.ID = 1

	msg2 := outbox.FakeMessage()
	msg2.ID = 2

	filter := outbox.MessageFilter{}
	limit := 10

	tests := []struct {
		name       string
		messages   []outbox.Message
		setupMocks func(readerMock *mocks.Reader, publisherMock *mocks.Publisher)
		stats      outbox.ForwardingStats
		wantErr    bool
	}{
		{
			name: "no messages",
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(nil, nil)
			},
		},
		{
			name:     "one message",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)

				readerMock.On("Mark", ctx, []int64{msg1.ID}).Return(int64(1), nil)
			},
			stats: outbox.ForwardingStats{
				Read:      1,
				Published: 1,
				Marked:    1,
			},
		},
		{
			name:     "two messages",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Mark", ctx, []int64{msg1.ID, msg2.ID}).Return(int64(2), nil)
			},
			stats: outbox.ForwardingStats{
				Read:      2,
				Published: 2,
				Marked:    2,
			},
		},
		{
			name:     "first fails",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(errors.New("failed"))
			},
			stats: outbox.ForwardingStats{
				Read:      1,
				Published: 0,
				Marked:    0,
			},
			wantErr: true,
		},
		{
			name:     "first okay, second fails",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(errors.New("failed"))
			},
			stats: outbox.ForwardingStats{
				Read:      2,
				Published: 1,
				Marked:    0,
			},
			wantErr: true,
		},
		{
			name:     "two messages, but only one marked",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Mark", ctx, []int64{msg1.ID, msg2.ID}).Return(int64(1), nil)
			},
			stats: outbox.ForwardingStats{
				Read:      2,
				Published: 2,
				Marked:    1,
			},
		},
		{
			name:     "two messages, mark fails",
			messages: outbox.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, filter, limit).Return(
					[]outbox.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Mark", ctx, []int64{msg1.ID, msg2.ID}).Return(int64(0), errors.New("failed"))
			},
			stats: outbox.ForwardingStats{
				Read:      2,
				Published: 2,
				Marked:    0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mocks
			readerMock := new(mocks.Reader)
			publisherMock := new(mocks.Publisher)

			// Create the forwarder
			forwarder, err := outbox.NewForwarder(readerMock, publisherMock)
			require.NoError(t, err)

			// Set up mocks
			tt.setupMocks(readerMock, publisherMock)

			// Call the method
			stats, err := forwarder.Forward(ctx, filter, limit)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.stats, stats)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.stats, stats)

			// Assert expectations
			readerMock.AssertExpectations(t)
			publisherMock.AssertExpectations(t)
		})
	}
}
