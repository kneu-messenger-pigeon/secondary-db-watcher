package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSendSecondaryDbLoadedEvent(t *testing.T) {
	loc := getTimeLocation()
	previousDatetime := time.Date(2023, 9, 1, 4, 0, 0, 0, loc)
	currentDatetime := time.Date(2023, 9, 2, 4, 0, 0, 0, loc)

	expectedError := errors.New("some error")

	payload, _ := json.Marshal(events.SecondaryDbLoadedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatetime,
		Year:                              currentDatetime.Year(),
	})

	expectedMessage := kafka.Message{
		Key:   []byte(events.SecondaryDbLoadedEventName),
		Value: payload,
	}

	t.Run("Success send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(nil)

		eventbus := Eventbus{writer: writer}
		err := eventbus.sendSecondaryDbLoadedEvent(currentDatetime, previousDatetime, currentDatetime.Year())

		assert.NoErrorf(t, err, "Not expect for error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})

	t.Run("Failed send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(expectedError)

		eventbus := Eventbus{writer: writer}
		err := eventbus.sendSecondaryDbLoadedEvent(currentDatetime, previousDatetime, currentDatetime.Year())

		assert.Errorf(t, err, "Expect for error")
		assert.Equal(t, expectedError, err, "Got unexpected error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})
}

func TestSendCurrentYearEvent(t *testing.T) {
	expectedYear := 2050
	expectedError := errors.New("some error")

	payload, _ := json.Marshal(events.CurrentYearEvent{
		Year: expectedYear,
	})

	expectedMessage := kafka.Message{
		Key:   []byte(events.CurrentYearEventName),
		Value: payload,
	}

	t.Run("Success send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(nil)

		eventbus := Eventbus{writer: writer}

		err := eventbus.sendCurrentYearEvent(expectedYear)

		assert.NoErrorf(t, err, "Not expect for error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})

	t.Run("Failed send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(expectedError)

		eventbus := Eventbus{writer: writer}
		err := eventbus.sendCurrentYearEvent(expectedYear)

		assert.Errorf(t, err, "Expect for error")
		assert.Equal(t, expectedError, err, "Got unexpected error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})
}
