package main

import (
	"errors"
	"fmt"
	"io"
	"time"
)

var BreakLoopError = errors.New("break loop")
var TooManyError = errors.New("too many error")

func runMainLoop(config Config, out io.Writer, iterationExecutor func() error) error {
	var err error

	errorCount := 0
	for true {
		err = iterationExecutor()

		if errors.Is(err, BreakLoopError) {
			break
		}

		if err != nil {
			fmt.Fprintln(out, getCurrentDatetime()+" "+err.Error())

			errorCount++
			if errorCount >= config.errorCountToBreak {
				fmt.Fprintln(out, "Too many mistakes: "+err.Error())
				err = TooManyError
				break
			}

			time.Sleep(config.pauseAfterError)
		} else {
			fmt.Fprintln(out, getCurrentDatetime()+" iteration done success")
			errorCount = 0
			time.Sleep(config.pauseAfterSuccess)
		}
	}

	return err
}

func getCurrentDatetime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
