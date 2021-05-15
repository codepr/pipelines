package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

type FileEventSource struct {
	filename string
}

func (f FileEventSource) Run() (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		file, err := os.Open(f.filename)
		if err != nil {
			errc <- err
			return
		}
		defer file.Close()
		scan := bufio.NewScanner(file)
		for scan.Scan() {
			// Write to the channel we will return
			// We additionally have to copy the content
			// of the slice returned by scan.Bytes() into
			// a new slice (using append()) before sending
			// it to another go-routine since scan.Bytes()
			// will re-use the slice it returned for
			// subsequent scans, which will garble up data
			// later if we don't put the content in a new one.
			out <- append([]byte(nil), scan.Bytes()...)
		}
		if scan.Err() != nil {
			errc <- scan.Err()
		}
		fmt.Println("Closed file reader channel")
	}()
	return out, errc
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := FileEventSource{os.Args[1]}
	aggregator := &VisitEventAggregator{1 * time.Hour, 64, source}
	c, errc := aggregator.Summarize(ctx)
	for record := range c {
		log.Println(record)
	}
	for err := range errc {
		log.Println(err)
	}
}
