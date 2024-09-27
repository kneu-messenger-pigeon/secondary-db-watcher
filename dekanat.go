package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"regexp"
	"strings"
	"time"
)

const FirebirdTimeFormat = "2006-01-02T15:04:05"

const StorageTimeFormat = time.RFC3339

const GetLastDatetimeQuery = "SELECT FIRST 1 CON_DATA FROM TSESS_LOG ORDER BY ID DESC"

const GetFirstLessonRegDateQuery = "SELECT FIRST 1 REGDATE FROM T_PRJURN ORDER BY REGDATE ASC"

func makeDbState(secondaryDekanatDb *sql.DB) (state dbState, err error) {
	state.ActualDatetime, err = getDbStateDatetime(secondaryDekanatDb)
	if err != nil {
		return state, errors.New("Failed to get last datetime from DB: " + err.Error())
	}

	state.EducationYear, err = getCurrentYear(secondaryDekanatDb)
	if err != nil {
		return state, errors.New("failed to detect current education year: " + err.Error())
	}

	return state, nil
}

func checkDekanatDb(secondaryDekanatDb *sql.DB, storage fileStorage.Interface, eventbus MetaEventbusInterface) error {
	currentState, err := makeDbState(secondaryDekanatDb)
	if err != nil {
		return errors.New("Failed to get DB state: " + err.Error())
	}

	var previousState dbState
	previousStateSerialized, err := storage.Get()

	if previousStateSerialized != nil && err == nil {
		err = json.Unmarshal(previousStateSerialized, &previousState)
	}

	if err != nil {
		return errors.New("Failed to get previous DB state from Storage: " + err.Error())
	}

	if previousState.isEqual(currentState) {
		return nil
	}

	currentStateSerialized, _ := json.Marshal(currentState)
	err = storage.Set(currentStateSerialized)
	if err != nil {
		return err
	}

	if currentState.EducationYear != previousState.EducationYear {
		err = eventbus.sendCurrentYearEvent(currentState.EducationYear)
		if err != nil {
			_ = storage.Set(previousStateSerialized)
			return errors.New("Failed to send Current year event to Kafka: " + err.Error())
		}
	}

	err = eventbus.sendSecondaryDbLoadedEvent(
		currentState.ActualDatetime, previousState.ActualDatetime,
		currentState.EducationYear,
	)
	if err != nil {
		_ = storage.Set(previousStateSerialized)
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
	rows := secondaryDekanatDb.QueryRow(GetLastDatetimeQuery)
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

func getCurrentYear(secondaryDekanatDb *sql.DB) (int, error) {
	var firstLessonRegDateString string
	rows := secondaryDekanatDb.QueryRow(GetFirstLessonRegDateQuery)
	if rows.Err() != nil {
		return 0, rows.Err()
	}

	err := rows.Scan(&firstLessonRegDateString)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("empty last date from DB: %s", err))
	}

	// git first 10 chars of string, like "2024-09-02"
	firstLessonRegDateString = firstLessonRegDateString[:10]
	firstLessonRegDate, err := time.ParseInLocation("2006-01-02", firstLessonRegDateString, time.Local)

	if err != nil {
		return 0, errors.New(fmt.Sprintf("failed to parse first lesson registration date: %s", err))
	}

	year := firstLessonRegDate.Year()
	// if first half of year - definitely it is education year start year ago
	if firstLessonRegDate.Month() < 8 {
		year--
	}

	if year < 2022 {
		return 0, errors.New(fmt.Sprintf("wrong education (should be 2022 or later): %d", year))
	}

	return year, nil
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

	if year < 2022 {
		return 0, errors.New(fmt.Sprintf("wrong education (should be 2022 or later): %d", year))
	}

	return year, nil
}
