package main

import (
	"errors"
	"os"
)

type StorageInterface interface {
	get() (string, error)
	set(value string) error
}

type Storage struct {
	file   string
	value  string
	loaded bool
}

func (storage Storage) get() (string, error) {
	if storage.loaded == false {
		storage.loaded = true
		if _, err := os.Stat(storage.file); errors.Is(err, os.ErrNotExist) {
			return "", nil
		}

		data, err := os.ReadFile(storage.file)
		if err != nil {
			return "", err
		}

		storage.value = string(data)
	}

	return storage.value, nil
}

func (storage *Storage) set(value string) error {
	if storage.loaded && storage.value == value {
		return nil
	}

	err := os.WriteFile(storage.file, []byte(value), 0644)
	if err != nil {
		return err
	}

	storage.value = value
	storage.loaded = true
	return nil
}
