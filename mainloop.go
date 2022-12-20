package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var BreakLoopError = errors.New("break loop")
var TooManyError = errors.New("too many error")

func runMainLoop(config Config, out io.Writer, iterationExecutor func() error) error {
	var err error
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(sig)

	errorCount := 0
	var pause time.Duration
	for {
		err = iterationExecutor()

		if errors.Is(err, BreakLoopError) {
			break
		}

		pause = config.pauseAfterSuccess
		if err != nil {
			pause = config.pauseAfterError
			fmt.Fprintln(out, getCurrentDatetime()+" "+err.Error())

			errorCount++
			if errorCount >= config.errorCountToBreak {
				fmt.Fprintln(out, "Too many mistakes: "+err.Error())
				err = TooManyError
				break
			}

		} else {
			fmt.Fprintln(out, getCurrentDatetime()+" iteration done success")
			errorCount = 0
		}

		select {
		case <-time.After(pause): // nothing
		case <-sig:
			fmt.Fprintln(out, "cancelled")
			return nil

		}

	}

	return err
}

func getCurrentDatetime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
