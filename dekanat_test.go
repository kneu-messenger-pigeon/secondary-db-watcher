package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	fileStorageMocks "github.com/kneu-messenger-pigeon/fileStorage/mocks"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func newDekanatDbMock(lastDatetime interface{}, firstLessonReg interface{}) *sql.DB {
	db, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
	}

	setQueryResult(mock, GetLastDatetimeQuery, lastDatetime)
	setQueryResult(mock, GetFirstLessonRegDateQuery, firstLessonReg)

	return db
}

func setQueryResult(mock sqlmock.Sqlmock, query string, returnValue interface{}) {
	switch returnValue.(type) {
	case error:
		mock.ExpectQuery(query).WillReturnError(returnValue.(error))

	case time.Time:
		Time := returnValue.(time.Time)

		mock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}).AddRow(Time.Format(FirebirdTimeFormat)),
		)

	case string:
		mock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}).AddRow(returnValue),
		)
	case nil:
		mock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"CON_DATA"}),
		)
	}
}

func TestGetDbStateDatetime(t *testing.T) {
	var db *sql.DB
	var expectedDatetime time.Time
	var expectedRegDate time.Time
	var expectedErr error

	var actualDatetime time.Time
	var actualErr error

	t.Run("valid datetime", func(t *testing.T) {
		expectedDatetime = time.Date(2022, 11, 2, 4, 0, 0, 0, time.Local)
		expectedRegDate = time.Date(2022, 9, 3, 0, 0, 0, 0, time.Local)
		db = newDekanatDbMock(expectedDatetime, expectedRegDate)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.NoError(t, actualErr)
		assert.Equalf(t, expectedDatetime, actualDatetime,
			"Expect getDbStateDatetime(db) = %s, actual: %s", expectedDatetime, actualDatetime,
		)
	})

	t.Run("remove milliseconds", func(t *testing.T) {
		expectedDatetime = time.Date(2022, 11, 2, 4, 0, 0, 0, time.Local)
		expectedRegDate = time.Date(2022, 9, 3, 0, 0, 0, 0, time.Local)

		expectedDatetimeString := "2022-11-02T04:00:00.123Z"
		db = newDekanatDbMock(expectedDatetimeString, expectedDatetime)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.NoError(t, actualErr)
		assert.Equalf(t, expectedDatetime, actualDatetime,
			"Expect getDbStateDatetime(db) = %s, actual: %s", expectedDatetime, actualDatetime,
		)
	})

	t.Run("invalid datetime", func(t *testing.T) {
		expectedErr = errors.New("cannot parse \"invalid\" as")
		db = newDekanatDbMock("invalid", nil)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Containsf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

	t.Run("error instead of datetime", func(t *testing.T) {
		expectedErr = errors.New("dummy error")
		db = newDekanatDbMock(expectedErr, nil)

		actualDatetime, actualErr = getDbStateDatetime(db)

		assert.Error(t, actualErr)
		assert.Containsf(t, actualErr.Error(), expectedErr.Error(),
			"Expect getDbStateDatetime(db) = nil, %s, actual: %s, %s", expectedErr, actualDatetime, actualErr,
		)
	})

	t.Run("empty result from DB", func(t *testing.T) {
		expectedErr = errors.New("empty last date from DB: sql: no rows in result set")
		db = newDekanatDbMock(nil, nil)

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
			// after 5 september 2023 year should be 2023
			time.Date(2023, 9, 0, 4, 0, 0, 0, loc):  2022,
			time.Date(2023, 9, 1, 4, 0, 0, 0, loc):  2023,
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
	var storageInstance *fileStorageMocks.Interface
	var producer *MockMetaEventbusInterface
	var previousState dbState
	var expectedState dbState

	var err error
	var expectedError error
	loc := time.Local

	var serializeState = func(state dbState) []byte {
		s, _ := json.Marshal(state)
		return s
	}

	t.Run("changeEducationYear", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2022, 6, 1, 4, 0, 0, 0, loc),
			EducationYear:  2021,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 15, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, expectedState.ActualDatetime)

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendCurrentYearEvent", 2023).Return(nil)
		producer.On(
			"sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		).Return(nil)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertCalled(
			t, "sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		)
		producer.AssertCalled(t, "sendCurrentYearEvent", 2023)
		storageInstance.AssertCalled(t, "Set", serializeState(expectedState))
	})

	t.Run("ErrorSendCurrentYearEvent", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2022, 6, 1, 4, 0, 0, 0, loc),
			EducationYear:  2021,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 15, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedError = errors.New("dummy error sendCurrentYearEvent")

		db = newDekanatDbMock(expectedState.ActualDatetime, expectedState.ActualDatetime)

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(nil)
		storageInstance.On("Set", serializeState(previousState)).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On("sendCurrentYearEvent", 2023).Return(expectedError)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat should fails with error")

		producer.AssertNotCalled(t, "sendSecondaryDbLoadedEvent")
		producer.AssertCalled(t, "sendCurrentYearEvent", 2023)
		storageInstance.AssertCalled(t, "Set", serializeState(expectedState))
		storageInstance.AssertCalled(t, "Set", serializeState(previousState))
	})

	t.Run("ChangeDatetime", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 11, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 12, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2023-09-02")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On(
			"sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		).Return(nil)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertCalled(
			t, "sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		)

		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertCalled(t, "Set", serializeState(expectedState))
	})

	t.Run("ErrorSendSecondaryDbLoadedEvent", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 11, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 12, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2023-09-02")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(nil)
		storageInstance.On("Set", serializeState(previousState)).Return(nil)

		expectedError = errors.New("dummy error sendCurrentYearEvent")

		producer = NewMockMetaEventbusInterface(t)
		producer.On(
			"sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		).Return(expectedError)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "expect checkDekanat fails")

		producer.AssertCalled(
			t, "sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		)
		producer.AssertNotCalled(t, "sendCurrentYearEvent")
		storageInstance.AssertCalled(t, "Set", serializeState(expectedState))
		storageInstance.AssertCalled(t, "Set", serializeState(previousState))
	})

	t.Run("NoChangeDatetime", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2023-09-02")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)

		producer = NewMockMetaEventbusInterface(t)
		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoErrorf(t, err, "checkDekanat failed with error: %s", err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbError", func(t *testing.T) {
		expectedError = errors.New("dummy error")

		db = newDekanatDbMock(expectedError, nil)
		storageInstance = fileStorageMocks.NewInterface(t)
		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(), "Expected %s, acutal %s", expectedError, err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbWrongDatetime", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock("2000-01-01T04:00:00Z", "2000-09-02")
		storageInstance = fileStorageMocks.NewInterface(t)

		producer = NewMockMetaEventbusInterface(t)

		expectedError = errors.New("failed to detect current education year")

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "Failed to get last datetime from DB: parsing time \"DUMMY_INVALID_DATETIME\" as \"2006-01-02T15:04:05+0")
		assert.Containsf(
			t, err.Error(), expectedError.Error(),
			"Expected %s, actual %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbNoEducationYear", func(t *testing.T) {
		db = newDekanatDbMock("2000-01-01T04:00:00Z", nil)
		storageInstance = fileStorageMocks.NewInterface(t)

		producer = NewMockMetaEventbusInterface(t)

		expectedError = errors.New("failed to detect current education year")

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err)
		assert.Containsf(
			t, err.Error(), expectedError.Error(),
			"Expected %s, actual %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbErrorGettingEducationYear", func(t *testing.T) {
		dbError := errors.New("dummy error")

		db = newDekanatDbMock("2000-01-01T04:00:00Z", dbError)
		storageInstance = fileStorageMocks.NewInterface(t)

		producer = NewMockMetaEventbusInterface(t)

		expectedError = errors.New("failed to detect current education year")

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err)
		assert.Containsf(
			t, err.Error(), expectedError.Error(),
			"Expected %s, actual %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbBadDateEducationYear", func(t *testing.T) {
		db = newDekanatDbMock("2000-01-01T04:00:00Z", "incorrect-date")
		storageInstance = fileStorageMocks.NewInterface(t)

		producer = NewMockMetaEventbusInterface(t)

		expectedError = errors.New("failed to detect current education year")

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err)
		assert.Containsf(
			t, err.Error(), expectedError.Error(),
			"Expected %s, actual %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("DekanatDbSecondSemesterDateEducationYear", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 4, 14, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 4, 15, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2024-04-15")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(nil)

		producer = NewMockMetaEventbusInterface(t)
		producer.On(
			"sendSecondaryDbLoadedEvent",
			expectedState.ActualDatetime, previousState.ActualDatetime, expectedState.EducationYear,
		).Return(nil)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.NoError(t, err)
		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 1)
		storageInstance.AssertNumberOfCalls(t, "Set", 1)
	})

	t.Run("StorageGetError", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2023-09-02")

		expectedError = errors.New("Failed to detect current education year")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(nil, expectedError)

		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(),
			"Expected %s, actгal %s", expectedError, err,
		)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertNumberOfCalls(t, "Set", 0)
	})

	t.Run("StorageSetError", func(t *testing.T) {
		previousState = dbState{
			ActualDatetime: time.Date(2023, 9, 1, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		expectedState = dbState{
			ActualDatetime: time.Date(2023, 9, 2, 4, 0, 0, 0, loc),
			EducationYear:  2023,
		}

		db = newDekanatDbMock(expectedState.ActualDatetime, "2023-09-02")
		storageInstance = fileStorageMocks.NewInterface(t)

		expectedError = errors.New("dummy set error")

		storageInstance = fileStorageMocks.NewInterface(t)
		storageInstance.On("Get").Return(serializeState(previousState), nil)
		storageInstance.On("Set", serializeState(expectedState)).Return(expectedError)

		producer = NewMockMetaEventbusInterface(t)

		err = checkDekanatDb(db, storageInstance, producer)

		assert.Error(t, err, "checkDekanat not failed with error")
		assert.Containsf(t, err.Error(), expectedError.Error(), "Expected %s, acutal %s", expectedError, err)

		producer.AssertNumberOfCalls(t, "sendSecondaryDbLoadedEvent", 0)
		producer.AssertNumberOfCalls(t, "sendCurrentYearEvent", 0)
		storageInstance.AssertCalled(t, "Set", serializeState(expectedState))
	})
}
