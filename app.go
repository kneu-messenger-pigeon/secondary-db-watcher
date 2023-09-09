package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/fileStorage"
	_ "github.com/nakagami/firebirdsql"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
)

const ExitCodeMainError = 1
const ExitCodeLoopIsBroken = 2
const ExitCodeTooManyErrorInLoop = 3

func runApp(out io.Writer) error {
	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}

	config, err := loadConfig(envFilename)
	if err != nil {
		return errors.New("Failed to load config: " + err.Error())
	}

	eventbus := MetaEventbus{
		out: out,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    events.MetaEventsTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}

	storage := fileStorage.Storage{
		File: config.storageFile,
	}

	secondaryDekanatDb, err := sql.Open(config.dekanatDbDriverName, config.secondaryDekanatDbDSN)
	if err != nil {
		return errors.New("Wrong connection configuration for secondary Dekanat DB: " + err.Error())
	}
	defer func() {
		eventbus.writer.Close()
		secondaryDekanatDb.Close()
	}()

	_, err = storage.Get()
	if err != nil {
		return errors.New(fmt.Sprintf(
			"Failed to load storage file %s - %s \n", config.storageFile, err,
		))
	}

	return runMainLoop(config, out, func() error {
		return checkDekanatDb(secondaryDekanatDb, &storage, &eventbus)
	})
}

func handleExitError(errStream io.Writer, err error) int {
	if err != nil {
		fmt.Fprintln(errStream, err)
	}

	if errors.Is(err, TooManyError) {
		return ExitCodeTooManyErrorInLoop
	}

	if errors.Is(err, BreakLoopError) {
		return ExitCodeLoopIsBroken
	}

	if err != nil {
		return ExitCodeMainError
	}

	return 0
}
