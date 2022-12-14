package main

import (
	"context"
	"encoding/json"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"time"
)

type EventbusInterface interface {
	sendSecondaryDbLoadedEvent(currentDatabaseStateDatetime time.Time, previousDatabaseStateDatetime time.Time, year int) error
	sendCurrentYearEvent(year int) error
}

type WriterInterface interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Eventbus struct {
	writer WriterInterface
}

func (eventbus Eventbus) writeMessage(eventName string, event interface{}) error {
	payload, _ := json.Marshal(event)
	return eventbus.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(eventName),
			Value: payload,
		},
	)
}

func (eventbus Eventbus) sendSecondaryDbLoadedEvent(currentDatabaseStateDatetime time.Time, previousDatabaseStateDatetime time.Time, year int) error {
	return eventbus.writeMessage(events.SecondaryDbLoadedEventName, events.SecondaryDbLoadedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatabaseStateDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatabaseStateDatetime,
		Year:                              year,
	})
}

func (eventbus Eventbus) sendCurrentYearEvent(year int) error {
	return eventbus.writeMessage(events.CurrentYearEventName, events.CurrentYearEvent{
		Year: year,
	})
}
