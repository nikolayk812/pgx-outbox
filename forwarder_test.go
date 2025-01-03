package outbox_test

import (
	"errors"
	"testing"

	outbox "github.com/nikolayk812/pgx-outbox"
	"github.com/nikolayk812/pgx-outbox/internal/fakes"
	"github.com/nikolayk812/pgx-outbox/internal/mocks"
	"github.com/nikolayk812/pgx-outbox/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwarder_Forward(t *testing.T) {
	t.Parallel()

	msg1 := fakes.FakeMessage()
	msg1.ID = 1

	msg2 := fakes.FakeMessage()
	msg2.ID = 2

	limit := 10

	tests := []struct {
		name       string
		messages   []types.Message
		setupMocks func(readerMock *mocks.Reader, publisherMock *mocks.Publisher)
		stats      types.ForwardStats
		wantErr    bool
	}{
		{
			name: "no messages",
			setupMocks: func(readerMock *mocks.Reader, _ *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(nil, nil)
			},
		},
		{
			name:     "one message",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)

				readerMock.On("Ack", ctx, []int64{msg1.ID}).Return(1, nil)
			},
			stats: types.ForwardStats{
				Read:      1,
				Published: 1,
				Acked:     1,
			},
		},
		{
			name:     "two messages",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Ack", ctx, []int64{msg1.ID, msg2.ID}).Return(2, nil)
			},
			stats: types.ForwardStats{
				Read:      2,
				Published: 2,
				Acked:     2,
			},
		},
		{
			name:     "first fails",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(errors.New("failed"))
			},
			stats: types.ForwardStats{
				Read:      1,
				Published: 0,
				Acked:     0,
			},
			wantErr: true,
		},
		{
			name:     "first okay, second fails",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(errors.New("failed"))
			},
			stats: types.ForwardStats{
				Read:      2,
				Published: 1,
				Acked:     0,
			},
			wantErr: true,
		},
		{
			name:     "two messages, but only one marked",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Ack", ctx, []int64{msg1.ID, msg2.ID}).Return(1, nil)
			},
			stats: types.ForwardStats{
				Read:      2,
				Published: 2,
				Acked:     1,
			},
		},
		{
			name:     "two messages, mark fails",
			messages: types.Messages{msg1},
			setupMocks: func(readerMock *mocks.Reader, publisherMock *mocks.Publisher) {
				readerMock.On("Read", ctx, limit).Return(
					[]types.Message{msg1, msg2}, nil)

				publisherMock.On("Publish", ctx, msg1).Return(nil)
				publisherMock.On("Publish", ctx, msg2).Return(nil)

				readerMock.On("Ack", ctx, []int64{msg1.ID, msg2.ID}).Return(0, errors.New("failed"))
			},
			stats: types.ForwardStats{
				Read:      2,
				Published: 2,
				Acked:     0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create mocks
			readerMock := new(mocks.Reader)
			publisherMock := new(mocks.Publisher)

			// Create the forwarder
			forwarder, err := outbox.NewForwarder(readerMock, publisherMock)
			require.NoError(t, err)

			// Set up mocks
			tt.setupMocks(readerMock, publisherMock)

			// Call the method
			stats, err := forwarder.Forward(ctx, limit)
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
