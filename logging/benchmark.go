package logging

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"imdea.org/redbelly/types"
)

const BufferLength = 100

const (
	EventElementAdded = iota
	EventEpochIncremented
)

type LogEvent struct {
	Time  time.Time
	Event int
	Data  []byte
}

type BenchmarkLogger struct {
	id        string
	writeChan chan *LogEvent
	writer    *csv.Writer
	stdLogger *log.Logger
}

func NewBenchmarkLogger(out io.Writer, callerId string) (*BenchmarkLogger, error) {
	return &BenchmarkLogger{
		id:        callerId,
		writeChan: make(chan *LogEvent, BufferLength),
		writer:    csv.NewWriter(out),
		stdLogger: log.New(os.Stdout, fmt.Sprintf("[BENCHMARK][%s]", callerId), log.Ltime),
	}, nil
}

func (bml *BenchmarkLogger) Start(ctx context.Context) {
	go func(ctx context.Context) {
		var e *LogEvent
		err := bml.writer.Write([]string{
			"timestamp",
			"node",
			"event",
			"data",
		})
	writeLoop:
		for {
			select {
			case e = <-bml.writeChan:
				err = bml.writer.Write([]string{
					fmt.Sprint(e.Time.UnixNano()),
					bml.id,
					fmt.Sprint(e.Event),
					string(e.Data),
				})
				bml.writer.Flush() //TODO remove from here when solve the error 2 bug on exit
				if err != nil {
					bml.stdLogger.Printf("E|Failed to write event to log: %s\n", err)
				}
			case <-ctx.Done():
				break writeLoop
			}
		}
		bml.writer.Flush()
		err = bml.writer.Error()
		if err != nil {
			bml.stdLogger.Printf("E|Failed to write log to file: %s", err)
		}
		fmt.Printf("Written")
	}(ctx)
}

func (bml *BenchmarkLogger) AddedElement(tx types.Transaction) {
	bml.writeChan <- &LogEvent{
		Time:  time.Now(),
		Event: EventElementAdded,
		Data:  []byte(tx.Hash().Fingerprint()),
	}
}

func (bml *BenchmarkLogger) EpochIncremented(n int, t time.Duration, m time.Duration) {
	bml.writeChan <- &LogEvent{
		Time:  time.Now(),
		Event: EventEpochIncremented,
		Data:  []byte(fmt.Sprintf("%v %f %f", n, t.Seconds()/float64(n), m.Seconds())),
	}
}
