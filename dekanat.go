package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const FirebirdTimeFormat = "2006-01-02T15:04:05"

func checkDekanatDb(secondaryDekanatDb *sql.DB, storage StorageInterface, eventbus MetaEventbusInterface) error {
	var currentDbStateDatetime time.Time
	var previousDbStateDatetime time.Time
	var err error

	currentDbStateDatetime, err = getDbStateDatetime(secondaryDekanatDb)
	if err != nil {
		return errors.New("Failed to get last datetime from DB: " + err.Error())
	}

	previousDbStateDatetimeString, err := storage.get()
	if previousDbStateDatetimeString != "" && err == nil {
		previousDbStateDatetime, err = time.ParseInLocation(time.UnixDate, previousDbStateDatetimeString, getTimeLocation())
	}

	if err != nil {
		return errors.New("Failed to get previous DB state datetime from Storage: " + err.Error())
	}

	if previousDbStateDatetime.Equal(currentDbStateDatetime) {
		return nil
	}

	currentEducationYear, err := extractEducationYear(currentDbStateDatetime)
	if err != nil {
		return errors.New("failed to detect current education year: " + err.Error())
	}

	err = storage.set(currentDbStateDatetime.Format(time.UnixDate))
	if err != nil {
		return err
	}

	var previousEducationYear int
	if !previousDbStateDatetime.IsZero() {
		previousEducationYear, _ = extractEducationYear(previousDbStateDatetime)
	}

	if currentEducationYear != previousEducationYear {
		err = eventbus.sendCurrentYearEvent(currentEducationYear)
		if err != nil {
			_ = storage.set(previousDbStateDatetime.Format(time.UnixDate))
			return errors.New("Failed to send Current year event to Kafka: " + err.Error())
		}
	}

	err = eventbus.sendSecondaryDbLoadedEvent(currentDbStateDatetime, previousDbStateDatetime, currentEducationYear)
	if err != nil {
		_ = storage.set(previousDbStateDatetime.Format(time.UnixDate))
		return errors.New("Failed to send Secondary DB loaded Event to Kafka: " + err.Error())
	}

	return nil
}

func getDbStateDatetime(secondaryDekanatDb *sql.DB) (time.Time, error) {
	err := secondaryDekanatDb.Ping()
	if err != nil {
		return time.Time{}, err
	}

	var lastDatetimeString string
	rows := secondaryDekanatDb.QueryRow("SELECT FIRST 1 CON_DATA FROM TSESS_LOG ORDER BY ID DESC")
	if rows.Err() != nil {
		return time.Time{}, rows.Err()
	}

	err = rows.Scan(&lastDatetimeString)
	if lastDatetimeString == "" || err != nil {
		return time.Time{}, errors.New(fmt.Sprintf("empty last date from DB: %s", err))
	}

	lastDatetimeString = strings.Replace(lastDatetimeString, "Z", "", 1)
	lastDatetimeString = strings.Replace(lastDatetimeString, "+02:00", "", 1)

	return time.ParseInLocation(FirebirdTimeFormat, lastDatetimeString, getTimeLocation())
}

func extractEducationYear(dbStateDatetime time.Time) (int, error) {
	if dbStateDatetime.IsZero() {
		return 0, errors.New("zero datetime for parse education year")
	}

	year := dbStateDatetime.Year()
	month := dbStateDatetime.Month()

	if month < 8 {
		year--
	}

	if year < 2022 {
		return 0, errors.New(fmt.Sprintf("wrong education (should be 2022 or later): %d", year))
	}

	return year, nil
}
