package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"strconv"
	"time"
)

type MetaEventbusInterface interface {
	sendSecondaryDbLoadedEvent(currentDatabaseStateDatetime time.Time, previousDatabaseStateDatetime time.Time, year int) error
	sendCurrentYearEvent(year int) error
}

type MetaEventbus struct {
	writer events.WriterInterface
	out    io.Writer
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
	if previousDatabaseStateDatetime.IsZero() {
		previousDatabaseStateDatetime = time.Date(
			year, 8, 1,
			0, 0, 0, 0, currentDatabaseStateDatetime.Location(),
		)
	}

	fmt.Fprintln(metaEventbus.out, "send SecondaryDbLoadedEvent ", currentDatabaseStateDatetime.Format(time.RFC3339))
	return metaEventbus.writeMessage(events.SecondaryDbLoadedEventName, events.SecondaryDbLoadedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatabaseStateDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatabaseStateDatetime,
		Year:                              year,
	})
}

func (metaEventbus MetaEventbus) sendCurrentYearEvent(year int) error {
	fmt.Fprintln(metaEventbus.out, "send sendCurrentYearEvent ", strconv.Itoa(year))
	return metaEventbus.writeMessage(events.CurrentYearEventName, events.CurrentYearEvent{
		Year: year,
	})
}
