package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type EventType string

const (
	Create EventType = "VisitCreate"
	Update EventType = "VisitUpdate"
)

const window int64 = 7200

type CreateEvent struct {
	Id         string    `json:"id"`
	UserId     string    `json:"userId"`
	DocumentId string    `json:"documentId"`
	CreatedAt  time.Time `json:"createdAt"`
}

type UpdateEvent struct {
	Id          string    `json:"id"`
	EngagedTime int       `json:"engagedTime"`
	Completion  float64   `json:"completion"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type EventT struct {
	MessageType EventType
}

type Event struct {
	EventT
	*CreateEvent
	*UpdateEvent
}

type DocumentSummary struct {
	DocumentId  string
	Start       time.Time
	End         time.Time
	Visits      int
	Uniques     int
	EngagedTime int
	Completion  int
}

func (d DocumentSummary) String() string {
	return fmt.Sprintf("%s,%s,%s,%d,%d,%d,%d", d.DocumentId, d.Start, d.End, d.Visits, d.Uniques, d.EngagedTime, d.Completion)
}

func (e *Event) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &e.EventT); err != nil {
		return err
	}
	switch e.EventT.MessageType {
	case Create:
		e.CreateEvent = &CreateEvent{}
		err := json.Unmarshal(data, &e.CreateEvent)
		if err != nil {
			return err
		}
	case Update:
		e.UpdateEvent = &UpdateEvent{}
		err := json.Unmarshal(data, &e.UpdateEvent)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized type value %q", e.EventT.MessageType)
	}
	return nil
}

func fileReaderGen(filename string) <-chan []byte {
	fileReadChan := make(chan []byte)
	go func() {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		} else {
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
				fileReadChan <- append([]byte(nil), scan.Bytes()...)
			}
			if scan.Err() != nil {
				log.Fatal(scan.Err())
			}
			close(fileReadChan)
			fmt.Println("Closed file reader channel")
		}
		file.Close()
	}()
	return fileReadChan
}

type EventResult struct {
	event Event
	err   error
}

func listEvents(ctx context.Context, lines <-chan []byte) <-chan EventResult {
	out := make(chan EventResult)
	go func() {
		var (
			wg  sync.WaitGroup
			sem = make(chan struct{}, 64)
		)
		for line := range lines {
			sem <- struct{}{}
			wg.Add(1)
			go func(line []byte) {
				defer func() {
					<-sem
					wg.Done()
				}()
				if len(line) == 0 {
					out <- EventResult{err: errors.Errorf("empty line")}
					return
				}
				var event Event
				err := json.Unmarshal(line, &event)
				select {
				case out <- EventResult{event: event, err: err}:
				case <-ctx.Done():
					return
				}
			}(line)
		}
		go func() {
			wg.Wait()
			close(out)
		}()
	}()
	return out
}

func summarize(ctx context.Context, events <-chan EventResult) (<-chan DocumentSummary, <-chan error) {
	type tuple struct {
		time    time.Time
		summary DocumentSummary
	}
	out := make(chan DocumentSummary)
	errc := make(chan error, 1)
	state := make(map[string]tuple)
	go func() {
		defer close(out)
		defer close(errc)
		for event := range events {
			if event.err != nil {
				continue
			}
			switch event.event.EventT.MessageType {
			case Create:
				startTime := time.Unix((event.event.CreatedAt.Unix()/window)*window, 0)
				summary := DocumentSummary{
					DocumentId:  event.event.DocumentId,
					Start:       startTime,
					End:         startTime.Add(2 * time.Hour),
					Visits:      1,
					Uniques:     1,
					EngagedTime: 1,
					Completion:  0.0,
				}
				state[event.event.CreateEvent.Id] = tuple{event.event.CreatedAt, summary}
				out <- summary
			case Update:
				tupleState, ok := state[event.event.UpdateEvent.Id]
				// discard it
				if !ok {
					continue
				}
				// 1 hour window check
				if event.event.UpdateEvent.UpdatedAt.Unix()-tupleState.time.Unix() < 3600 {
					tupleState.summary.Visits = 0 // We don't count updates as visits
					tupleState.summary.EngagedTime = event.event.UpdateEvent.EngagedTime
					tupleState.summary.Completion = int(event.event.UpdateEvent.Completion)
					// We update the visitId -> (createdAt, doc) value in the docs map
					out <- tupleState.summary
				} else {
					delete(state, event.event.UpdateEvent.Id)
				}
			}
		}
	}()
	return out, errc
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	summaries, errc := summarize(ctx, listEvents(ctx, fileReaderGen(os.Args[1])))
	for summary := range summaries {
		log.Printf("%s\n", summary)
	}
	err, ok := <-errc
	if ok {
		log.Fatal(err)
	}
}
