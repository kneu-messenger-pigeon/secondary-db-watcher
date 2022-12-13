// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import mock "github.com/stretchr/testify/mock"

// MockStorageInterface is an autogenerated mock type for the StorageInterface type
type MockStorageInterface struct {
	mock.Mock
}

// get provides a mock function with given fields:
func (_m *MockStorageInterface) get() (string, error) {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// set provides a mock function with given fields: value
func (_m *MockStorageInterface) set(value string) error {
	ret := _m.Called(value)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewMockStorageInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockStorageInterface creates a new instance of MockStorageInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockStorageInterface(t mockConstructorTestingTNewMockStorageInterface) *MockStorageInterface {
	mock := &MockStorageInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
