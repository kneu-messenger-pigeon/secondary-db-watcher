package main

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kneu-messenger-pigeon/fileStorage"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

var expectedGetDatetimeQuery = "SELECT FIRST 1 CON_DATA FROM TSESS_LOG ORDER BY ID DESC"

func newDekanatDbMock(expectedResult interface{}) *sql.DB {
	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
	}

	switch expectedResult.(type) {
	case error:
		mock.ExpectQuery(expectedGetDatetimeQuery).WillReturnError(expectedResult.(error))

	case time.Time:
		Time := expectedResult.(time.Time)

		mock.ExpectQuery(expectedGetDatetimeQuery).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}).AddRow(Time.Format(FirebirdTimeFormat)),
		)

	case string:
		mock.ExpectQuery(expectedGetDatetimeQuery).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}).AddRow(expectedResult),
		)
	case nil:
		mock.ExpectQuery(expectedGetDatetimeQuery).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}),
		)
	}

	return db
}

func TestGetDbStateDatetime(t *testing.T) {
	var db *sql.DB
	var expectedDatetime time.Time
	var expectedErr error

	var actualDatetime time.Time
	var actualErr error

	t.Run("valid datetime", func(t *testing.T) {
		expectedDatetime = time.Date(2022, 11, 2, 4, 0, 0, 0, time.Local)
		db = newDekanatDbMock(expectedDatetime)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.NoError(t, actualErr)
		assert.Equalf(t, expectedDatetime, actualDatetime,
			"Expect getDbStateDatetime(db) = %s, actual: %s", expectedDatetime, actualDatetime,
		)
	})

	t.Run("remove milliseconds", func(t *testing.T) {
		expectedDatetime = time.Date(2022, 11, 2, 4, 0, 0, 0, time.Local)
		expectedDatetimeString := "2022-11-02T04:00:00.123Z"
		db = newDekanatDbMock(expectedDatetimeString)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.NoError(t, actualErr)
		assert.Equalf(t, expectedDatetime, actualDatetime,
			"Expect getDbStateDatetime(db) = %s, actual: %s", expectedDatetime, actualDatetime,
		)
	})

	t.Run("invalid datetime", func(t *testing.T) {
		expectedErr = errors.New("cannot parse \"invalid\" as")
		db = newDekanatDbMock("invalid")

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Containsf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

	t.Run("error instead of datetime", func(t *testing.T) {
		expectedErr = errors.New("dummy error")
		db = newDekanatDbMock(expectedErr)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Containsf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

	t.Run("empty result from DB", func(t *testing.T) {
		expectedErr = errors.New("empty last date from DB: sql: no rows in result set")
		db = newDekanatDbMock(nil)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Equalf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

	t.Run("db ping fails", func(t *testing.T) {
		expectedErr = errors.New("ping error")

		db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		mock.ExpectPing().WillReturnError(expectedErr)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Equalf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

}

func TestExtractEducationYearValidInput(t *testing.T) {
	var actualYear int
	var err error
	loc := time.Local

	t.Run("valid input for extractEducationYear", func(t *testing.T) {
		testCases := map[time.Time]int{
			time.Date(2022, 11, 1, 4, 0, 0, 0, loc): 2022,
			time.Date(2023, 1, 15, 4, 0, 0, 0, loc): 2022,
			time.Date(2023, 6, 15, 4, 0, 0, 0, loc): 2022,
			time.Date(2023, 8, 1, 4, 0, 0, 0, loc):  2022,
			time.Date(2023, 9, 9, 4, 0, 0, 0, loc):  2022,
			// after 10 september 2023 year should be 2023
			time.Date(2023, 9, 10, 4, 0, 0, 0, loc): 2023,
			time.Date(2023, 11, 1, 4, 0, 0, 0, loc): 2023,
		}

		for testDatetime, expectedYear := range testCases {
			actualYear, err = extractEducationYear(testDatetime)

			assert.NoErrorf(t, err, "Failed to parse %s: %s", testDatetime, err)
			assert.Equalf(
				t, expectedYear, actualYear,
				`Expected extractEducationYear("%s") = %d, actual: %d`, testDatetime, expectedYear, actualYear,
			)
		}
	})

	t.Run("invalid input for extractEducationYear", func(t *testing.T) {
		testCases := [2]time.Time{
			time.Date(1990, 1, 1, 4, 0, 0, 0, loc),
		}

		for _, testDatetime := range testCases {
			actualYear, err = extractEducationYear(testDatetime)

			assert.Errorf(t, err, "Expected error on extractEducationYear(\"%s\"), acutal no error", testDatetime)
			assert.Emptyf(t, actualYear, `Expected extractEducationYear("%s") = 0, actual: %d`, testDatetime, actualYear)
		}
	})

}

func TestCheckDekanatDb(t *testing.T) {
	var db *sql.DB
	var storageInstance *fileStorage.MockInterface
	var producer *MockMetaEventbusInterface
	var previousDatetime time.Time
	var expectedDatetime time.Time
	var previousDatetimeString string
	var expectedDatetimeString string
	var err error
	var expectedError error
	loc := time.Local

	t.Run("changeEducationYear", func(t *testing.T) {
		previousDatetime = time.Date(2022, 6, 1, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 1, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)
		storageInstance.On("Set", expectedDatetimeString).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendCurrentYearEvent", 2023).Return(nil)
		producer.On("sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year()).Return(nil)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertCalled(t, "sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year())
		producer.AssertCalled(t, "sendCurrentYearEvent", 2023)
		storageInstance.AssertCalled(t, "Set", expectedDatetimeString)
	})

	t.Run("ErrorSendCurrentYearEvent", func(t *testing.T) {
		previousDatetime = time.Date(2022, 6, 1, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 1, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		expectedError = errors.New("dummy error sendCurrentYearEvent")

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)
		storageInstance.On("Set", expectedDatetimeString).Return(nil)
		storageInstance.On("Set", previousDatetimeString).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendCurrentYearEvent", 2023).Return(expectedError)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat should fails with error")

		producer.AssertNotCalled(t, "sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year())
		producer.AssertCalled(t, "sendCurrentYearEvent", 2023)
		storageInstance.AssertCalled(t, "Set", expectedDatetimeString)
		storageInstance.AssertCalled(t, "Set", previousDatetimeString)
	})

	t.Run("ChangeDatetime", func(t *testing.T) {
		previousDatetime = time.Date(2023, 9, 1, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)
		storageInstance.On("Set", expectedDatetimeString).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year()).Return(nil)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertCalled(t, "sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year())
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertCalled(t, "Set", expectedDatetimeString)
	})

	t.Run("ErrorSendSecondaryDbLoadedEvent", func(t *testing.T) {
		previousDatetime = time.Date(2023, 9, 1, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		expectedError = errors.New("dummy error sendCurrentYearEvent")

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)
		storageInstance.On("Set", expectedDatetimeString).Return(nil)
		storageInstance.On("Set", previousDatetimeString).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year()).Return(expectedError)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "expect checkDekanat fails")

		producer.AssertCalled(t, "sendSecondaryDbLoadedEvent", expectedDatetime, previousDatetime, expectedDatetime.Year())
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertCalled(t, "Set", expectedDatetimeString)
		storageInstance.AssertCalled(t, "Set", previousDatetimeString)
	})

	t.Run("NoChangeDatetime", func(t *testing.T) {
		previousDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)

		producer = NewMockMetaEventbusInterface(t)
		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "set", 0)
	})

	t.Run("DekanatDbError", func(t *testing.T) {
		expectedError = errors.New("dummy error")

		db = newDekanatDbMock(expectedError)
		storageInstance = fileStorage.NewMockInterface(t)
		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(), "Expected %s, acutal %s", expectedError, err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "set", 0)
	})

	t.Run("DekanatDbWrongDatetime", func(t *testing.T) {
		previousDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)
		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)

		expectedError = errors.New("failed to detect current education year")

		db = newDekanatDbMock("2000-01-01T04:00:00Z")
		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)

		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "Failed to get last datetime from DB: parsing time \"DUMMY_INVALID_DATETIME\" as \"2006-01-02T15:04:05+0")
		assert.Containsf(
			t, err.Error(), expectedError.Error(),
			"Expected %s, actual %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "set", 0)
	})

	t.Run("StorageGetError", func(t *testing.T) {
		expectedDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		expectedError = errors.New("Failed to detect current education year")

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return("", expectedError)

		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(),
			"Expected %s, actгal %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "set", 0)
	})

	t.Run("StorageSetError", func(t *testing.T) {
		previousDatetime = time.Date(2023, 9, 1, 4, 0, 0, 0, loc)
		expectedDatetime = time.Date(2023, 9, 2, 4, 0, 0, 0, loc)

		previousDatetimeString = previousDatetime.Format(StorageTimeFormat)
		expectedDatetimeString = expectedDatetime.Format(StorageTimeFormat)

		expectedError = errors.New("dummy set error")

		db = newDekanatDbMock(expectedDatetime)

		storageInstance = fileStorage.NewMockInterface(t)
		storageInstance.On("Get").Return(previousDatetimeString, nil)
		storageInstance.On("Set", expectedDatetimeString).Return(expectedError)

		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(), "Expected %s, acutal %s", expectedError, err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertCalled(t, "Set", expectedDatetimeString)
	})

}
