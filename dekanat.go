package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"regexp"
	"strings"
	"time"
)

const FirebirdTimeFormat = "2006-01-02T15:04:05"

const StorageTimeFormat = time.RFC3339

func checkDekanatDb(secondaryDekanatDb *sql.DB, storage fileStorage.Interface, eventbus MetaEventbusInterface) error {
	var currentDbStateDatetime time.Time
	var previousDbStateDatetime time.Time
	var err error

	currentDbStateDatetime, err = getDbStateDatetime(secondaryDekanatDb)
	if err != nil {
		return errors.New("Failed to get last datetime from DB: " + err.Error())
	}

	previousDbStateDatetimeString, err := storage.Get()
	if previousDbStateDatetimeString != "" && err == nil {
		previousDbStateDatetime, err = time.ParseInLocation(StorageTimeFormat, previousDbStateDatetimeString, time.Local)
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

	err = storage.Set(currentDbStateDatetime.Format(StorageTimeFormat))
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
			_ = storage.Set(previousDbStateDatetime.Format(StorageTimeFormat))
			return errors.New("Failed to send Current year event to Kafka: " + err.Error())
		}
	}

	err = eventbus.sendSecondaryDbLoadedEvent(currentDbStateDatetime, previousDbStateDatetime, currentEducationYear)
	if err != nil {
		_ = storage.Set(previousDbStateDatetime.Format(StorageTimeFormat))
		return errors.New("Failed to send Secondary DB loaded Event to Kafka: " + err.Error())
	}

	return nil
}

// drop "+02:00" , "+03:00" etc in the end
var removeTimeZone = regexp.MustCompile(`\+[0-9]{2}:[0-9]{2}$`)
var removeMilliseconds = regexp.MustCompile(`\.[0-9]{3}`)

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
	lastDatetimeString = removeTimeZone.ReplaceAllString(lastDatetimeString, "")
	lastDatetimeString = removeMilliseconds.ReplaceAllString(lastDatetimeString, "")

	return time.ParseInLocation(FirebirdTimeFormat, lastDatetimeString, time.Local)
}

func extractEducationYear(dbStateDatetime time.Time) (int, error) {
	if dbStateDatetime.IsZero() {
		return 0, errors.New("zero datetime for parse education year")
	}

	year := dbStateDatetime.Year()
	month := dbStateDatetime.Month()

	if month < 9 {
		year--
	}

	if month == 9 && dbStateDatetime.Day() < 1 {
		year--
	}

	if year < 2022 {
		return 0, errors.New(fmt.Sprintf("wrong education (should be 2022 or later): %d", year))
	}

	return year, nil
}
