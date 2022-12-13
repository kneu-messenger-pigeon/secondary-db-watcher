package main

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRunApp(t *testing.T) {
	t.Run("Run with mock config", func(t *testing.T) {
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", expectedConfig.secondaryDekanatDbDSN)
		_ = os.Setenv("STORAGE_FILE", expectedConfig.storageFile)
		_ = os.Setenv("PAUSE_AFTER_SUCCESS", "1")
		_ = os.Setenv("PAUSE_AFTER_ERROR", "1")
		_ = os.Setenv("ERROR_COUNT_TO_BREAK", "1")

		var out bytes.Buffer
		err := runApp(&out)
		output := out.String()

		assert.ErrorIs(t, err, TooManyError, "Expected for TooManyError, got %s", err)
		assert.Containsf(t, output, "Failed to get last datetime from DB", "Expected for Dekanat DB connect error: got: %s", output)
	})

	t.Run("Run with wrong env file", func(t *testing.T) {
		previousWd, err := os.Getwd()
		assert.NoErrorf(t, err, "Failed to get working dir: %s", err)
		tmpDir := os.TempDir() + "/secondary-db-watcher-run-dir"
		tmpEnvFilepath := tmpDir + "/.env"

		defer func() {
			_ = os.Chdir(previousWd)
			_ = os.Remove(tmpEnvFilepath)
			_ = os.Remove(tmpDir)
		}()

		if _, err := os.Stat(tmpDir); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpDir, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp dir %s: %s", tmpDir, err)
		}
		if _, err := os.Stat(tmpEnvFilepath); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpEnvFilepath, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp  %s/.env: %s", tmpDir, err)
		}

		err = os.Chdir(tmpDir)
		assert.NoErrorf(t, err, "Failed to change working dir: %s", err)

		var out bytes.Buffer
		err = runApp(&out)
		assert.Error(t, err, "Expected for error")
		assert.Containsf(
			t, err.Error(), "Failed to load config",
			"Expected for Load config error, got: %s", err,
		)
	})

	t.Run("Run with wrong sql driver", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "dummy-not-exist")
		defer os.Unsetenv("DEKANAT_DB_DRIVER_NAME")

		var out bytes.Buffer
		err := runApp(&out)

		expectedError := "Wrong connection configuration for secondary Dekanat DB: sql: unknown driver \"dummy-not-exist\" (forgotten import?)"

		assert.Error(t, err, "Expected for error")
		assert.Equalf(t, expectedError, err.Error(), "Expected for another error, got %s", err)
	})

	t.Run("Run with wrong storage file", func(t *testing.T) {
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", expectedConfig.secondaryDekanatDbDSN)
		_ = os.Setenv("STORAGE_FILE", os.TempDir())
		_ = os.Setenv("PAUSE_AFTER_SUCCESS", "1")
		_ = os.Setenv("PAUSE_AFTER_ERROR", "1")
		_ = os.Setenv("ERROR_COUNT_TO_BREAK", "1")

		var out bytes.Buffer
		err := runApp(&out)
		assert.Error(t, err, "Expected for error")
		assert.Containsf(
			t, err.Error(), "Failed to load storage file",
			"Expected for Failed to load storage file, got: %s", err,
		)
	})
}

func TestHandleExitError(t *testing.T) {
	t.Run("Handle exit error", func(t *testing.T) {
		var actualExitCode int
		var out bytes.Buffer

		testCases := map[error]int{
			errors.New("dummy error"): ExitCodeMainError,
			TooManyError:              ExitCodeTooManyErrorInLoop,
			BreakLoopError:            ExitCodeLoopIsBroken,
			nil:                       0,
		}

		for err, expectedCode := range testCases {
			out.Reset()
			actualExitCode = handleExitError(&out, err)

			assert.Equalf(
				t, expectedCode, actualExitCode,
				"Expect handleExitError(%v) = %d, actual: %d",
				err, expectedCode, actualExitCode,
			)
			if err == nil {
				assert.Empty(t, out.String(), "Error is not empty")
			} else {
				assert.Contains(t, out.String(), err.Error(), "error output hasn't error description")
			}
		}
	})
}
