package main

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestMainLoopThreeSuccessIterations(t *testing.T) {
	config := Config{
		secondaryDekanatDbDSN: "dummy",
		storageFile:           "dummy",
		pauseAfterSuccess:     0,
		pauseAfterError:       0,
		errorCountToBreak:     3,
	}

	wantExecutedCount := 3
	functionExecutedCount := 0

	executeIteration := func() error {
		functionExecutedCount++
		if functionExecutedCount >= wantExecutedCount {
			return BreakLoopError
		}
		return nil
	}

	var out bytes.Buffer
	err := runMainLoop(config, &out, executeIteration)
	output := out.String()

	assert.Emptyf(t, output, "Expected empty output, receive: %s", output)
	assert.Equalf(
		t, wantExecutedCount, functionExecutedCount,
		"Iteration Function is not executed expected amount times: %q, want %q",
		functionExecutedCount, wantExecutedCount,
	)
	assert.ErrorIsf(
		t, err, BreakLoopError,
		"Expected for BreakLoop, got %s.", err,
	)
}

func TestMainLoopThreeErrorIterationsAndBreak(t *testing.T) {
	config := Config{
		secondaryDekanatDbDSN: "dummy",
		storageFile:           "dummy",
		pauseAfterSuccess:     0,
		pauseAfterError:       0,
		errorCountToBreak:     3,
	}

	maxExecutedCount := 5
	expectedExecutedCount := config.errorCountToBreak
	functionExecutedCount := 0

	executeIteration := func() error {
		functionExecutedCount++
		if functionExecutedCount >= maxExecutedCount {
			return BreakLoopError
		}
		return errors.New("dummy error")
	}

	var out bytes.Buffer
	err := runMainLoop(config, &out, executeIteration)

	output := out.String()

	assert.Equalf(
		t, expectedExecutedCount, functionExecutedCount,
		"Iteration Function is not executed expected amount times: %q, want %q",
		functionExecutedCount, expectedExecutedCount,
	)
	assert.ErrorIs(
		t, err, TooManyError,
		"Expected forTooManyError, got %d.", err,
	)

	assert.Contains(t, output, "dummy error", "No dummy error in output")

	dummyErrorCount := strings.Count(output, "dummy error")
	assert.Equalf(
		t, functionExecutedCount+1, dummyErrorCount,
		"Not enough amount of dummy error in output. Expected: %d, actual: %d",
		functionExecutedCount+1, dummyErrorCount,
	)
	assert.Contains(t, output, "Too many mistakes", "No Too many mistakes in output")
}

func TestMainLoopPauseOnSuccess(t *testing.T) {
	config := Config{
		secondaryDekanatDbDSN: "dummy",
		storageFile:           "dummy",
		pauseAfterSuccess:     100 * time.Microsecond,
		pauseAfterError:       0,
		errorCountToBreak:     3,
	}

	maxExecutedCount := 3
	expectedExecutedCount := maxExecutedCount
	functionExecutedCount := 0

	executeIteration := func() error {
		functionExecutedCount++
		if functionExecutedCount >= maxExecutedCount {
			return BreakLoopError
		}
		return nil
	}

	var out bytes.Buffer

	start := time.Now()
	err := runMainLoop(config, &out, executeIteration)
	executionTime := time.Since(start)

	assert.Equalf(
		t, expectedExecutedCount, functionExecutedCount,
		"Iteration Function is not executed expected amount times: %q, want %q",
		functionExecutedCount, expectedExecutedCount,
	)
	assert.ErrorIs(t, err, BreakLoopError, "Expected for BreakLoopError, got %s.", err)

	expectedExecutionTime := config.pauseAfterSuccess * time.Duration(maxExecutedCount-1)
	assert.Greaterf(
		t, executionTime, expectedExecutionTime,
		"Success pause is not applied: Expect for execution greater that %s, actual exectution time: %s",
		expectedExecutionTime, executionTime,
	)
}

func TestMainLoopPauseOnError(t *testing.T) {
	config := Config{
		secondaryDekanatDbDSN: "dummy",
		storageFile:           "dummy",
		pauseAfterSuccess:     0,
		pauseAfterError:       100 * time.Microsecond,
		errorCountToBreak:     3,
	}

	maxExecutedCount := 3
	expectedExecutedCount := maxExecutedCount
	functionExecutedCount := 0

	executeIteration := func() error {
		functionExecutedCount++
		if functionExecutedCount >= maxExecutedCount {
			return BreakLoopError
		}
		return errors.New("dummy error")
	}

	var out bytes.Buffer

	start := time.Now()
	err := runMainLoop(config, &out, executeIteration)

	executionTime := time.Since(start)

	assert.Equalf(
		t, expectedExecutedCount, functionExecutedCount,
		"Iteration Function is not executed expected amount times: %q, want %q",
		functionExecutedCount, expectedExecutedCount,
	)
	assert.ErrorIs(t, err, BreakLoopError, "Expected for ExitCodeLoopIsBroken, got %d.", err)

	expectedExecutionTime := config.pauseAfterError * time.Duration(maxExecutedCount-1)

	assert.Greaterf(
		t, executionTime, expectedExecutionTime,
		"Success pause is not applied: Expect for execution greater that %s, actual exectution time: %s",
		expectedExecutionTime, executionTime,
	)
}

func TestGetCurrentDatetime(t *testing.T) {
	expected := time.Now()
	expected = time.Date(
		expected.Year(), expected.Month(), expected.Day(),
		expected.Hour(), expected.Minute(), expected.Second(),
		0, time.UTC,
	)

	actual, err := time.Parse("2006-01-02 15:04:05", getCurrentDatetime())

	assert.NoErrorf(t, err, "Failed to parse string: %s", err)
	assert.Equalf(t, expected, actual, "Expected current time %s, acutal %s", expected, actual)
}
