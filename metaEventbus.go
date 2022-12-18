package main

import (
	"context"
	"encoding/json"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"time"
)

type MetaEventbusInterface interface {
	sendSecondaryDbLoadedEvent(currentDatabaseStateDatetime time.Time, previousDatabaseStateDatetime time.Time, year int) error
	sendCurrentYearEvent(year int) error
}

type MetaEventbus struct {
	writer events.WriterInterface
}

func (metaEventbus MetaEventbus) writeMessage(eventName string, event interface{}) error {
	payload, _ := json.Marshal(event)
	return metaEventbus.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(eventName),
			Value: payload,
		},
	)
}

func (metaEventbus MetaEventbus) sendSecondaryDbLoadedEvent(currentDatabaseStateDatetime time.Time, previousDatabaseStateDatetime time.Time, year int) error {
	return metaEventbus.writeMessage(events.SecondaryDbLoadedEventName, events.SecondaryDbLoadedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatabaseStateDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatabaseStateDatetime,
		Year:                              year,
	})
}

func (metaEventbus MetaEventbus) sendCurrentYearEvent(year int) error {
	return metaEventbus.writeMessage(events.CurrentYearEventName, events.CurrentYearEvent{
		Year: year,
	})
}
