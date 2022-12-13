package main

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"time"
)

type Config struct {
	dekanatDbDriverName   string
	kafkaHost             string
	secondaryDekanatDbDSN string
	storageFile           string
	pauseAfterSuccess     time.Duration
	pauseAfterError       time.Duration
	errorCountToBreak     int
}

func loadConfig(envFilename string) (Config, error) {
	if envFilename != "" {
		err := godotenv.Load(envFilename)
		if err != nil {
			return Config{}, errors.New(fmt.Sprintf("Error loading %s file: %s", envFilename, err))
		}
	}

	pauseAfterSuccess, err := strconv.ParseInt(os.Getenv("PAUSE_AFTER_SUCCESS"), 10, 0)
	if pauseAfterSuccess == 0 || err != nil {
		pauseAfterSuccess = 600
	}

	pauseAfterError, err := strconv.ParseInt(os.Getenv("PAUSE_AFTER_ERROR"), 10, 0)
	if pauseAfterError == 0 || err != nil {
		pauseAfterError = 60
	}

	errorCountToBreak, err := strconv.Atoi(os.Getenv("ERROR_COUNT_TO_BREAK"))
	if errorCountToBreak == 0 || err != nil {
		errorCountToBreak = 3
	}

	config := Config{
		dekanatDbDriverName:   os.Getenv("DEKANAT_DB_DRIVER_NAME"),
		secondaryDekanatDbDSN: os.Getenv("SECONDARY_DEKANAT_DB_DSN"),
		kafkaHost:             os.Getenv("KAFKA_HOST"),
		storageFile:           os.Getenv("STORAGE_FILE"),
		pauseAfterSuccess:     time.Second * time.Duration(pauseAfterSuccess),
		pauseAfterError:       time.Second * time.Duration(pauseAfterError),
		errorCountToBreak:     errorCountToBreak,
	}

	if config.dekanatDbDriverName == "" {
		config.dekanatDbDriverName = "firebirdsql"
	}

	if config.secondaryDekanatDbDSN == "" {
		return Config{}, errors.New("empty SECONDARY_DEKANAT_DB_DSN")
	}

	if config.kafkaHost == "" {
		return Config{}, errors.New("empty KAFKA_HOST")
	}

	if config.storageFile == "" {
		config.storageFile = "storage.txt"
	}

	return config, nil
}
