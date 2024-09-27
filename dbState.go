package main

import "time"

type dbState struct {
	ActualDatetime time.Time
	EducationYear  int
}

func (a dbState) isEqual(b dbState) bool {
	return a.EducationYear == b.EducationYear && a.ActualDatetime.Equal(b.ActualDatetime)
}
