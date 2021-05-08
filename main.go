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

const window int64 = 3600

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
	EngagedTime float64
	Completion  int
}

func (d DocumentSummary) String() string {
	return fmt.Sprintf("%s,%s,%s,%d,%d,%f,%d", d.DocumentId, d.Start, d.End, d.Visits, d.Uniques, d.EngagedTime, d.Completion)
}

type PartialRecord struct {
	summary DocumentSummary
	event   Event
	instant time.Time
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

func summarize(ctx context.Context, events <-chan EventResult) (<-chan PartialRecord, <-chan error) {
	type tuple struct {
		time    time.Time
		summary DocumentSummary
	}
	out := make(chan PartialRecord)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		state := make(map[string]tuple)
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
					End:         startTime.Add(1 * time.Hour),
					Visits:      1,
					Uniques:     1,
					EngagedTime: 0.0,
					Completion:  0,
				}
				state[event.event.CreateEvent.Id] = tuple{event.event.CreatedAt, summary}
				out <- PartialRecord{summary, event.event, event.event.CreatedAt}
			case Update:
				tupleState, ok := state[event.event.UpdateEvent.Id]
				// discard it
				if !ok {
					continue
				}
				// 1 hour window check
				if event.event.UpdateEvent.UpdatedAt.Unix()-tupleState.time.Unix() < window {
					tupleState.summary.Visits = 0 // We don't count updates as visits
					tupleState.summary.EngagedTime = float64(event.event.UpdateEvent.EngagedTime)
					tupleState.summary.Completion = int(event.event.UpdateEvent.Completion)
					// We update the visitId -> (createdAt, doc) value in the docs map
					out <- PartialRecord{tupleState.summary, event.event, tupleState.time}
					state[event.event.UpdateEvent.Id] = tupleState // redundant
				} else {
					delete(state, event.event.UpdateEvent.Id)
				}
			}
		}
	}()
	return out, errc
}

func updateStats(ctx context.Context, events <-chan PartialRecord) <-chan PartialRecord {
	out := make(chan PartialRecord)
	type docstats struct {
		engagedTime int
		completion  int
	}
	type visitstats struct {
		engagedTime int
		completion  float64
	}
	go func() {
		defer close(out)
		state := make(map[string]docstats)
		visits := make(map[string]visitstats)
		for event := range events {
			switch event.event.EventT.MessageType {
			case Create:
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			case Update:
				// A `VisitUpdate`, first we calculate the current total for engagedTime and
				// completion
				vstats, ok := visits[event.event.UpdateEvent.Id]
				if !ok {
					vstats = visitstats{0, 0}
				}
				dstatsKey := fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.DocumentId)
				dstats, ok := state[dstatsKey]
				if !ok {
					dstats = docstats{0, 0}
				}
				totalEngagedTime := dstats.engagedTime + (event.event.EngagedTime - vstats.engagedTime)
				totalCompletion := dstats.completion + int(event.event.Completion)
				// Then we update the states for the next round
				state[dstatsKey] = docstats{totalEngagedTime, totalCompletion}
				visits[event.event.UpdateEvent.Id] = visitstats{event.event.EngagedTime, event.event.Completion}
				//finally we emit the updated `DocumentSummary` event
				event.summary.EngagedTime = float64(totalEngagedTime) / float64(window)
				event.summary.Completion = totalCompletion
				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func uniqueVisits(ctx context.Context, events <-chan PartialRecord) <-chan PartialRecord {
	out := make(chan PartialRecord)
	go func() {
		defer close(out)
		state := make(map[string]map[string]bool)
		for event := range events {
			var (
				unique = 0
				key    = fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.DocumentId)
			)
			switch event.event.EventT.MessageType {
			case Create:
				usersSet, ok := state[key]
				if !ok {
					state[key] = make(map[string]bool)
					state[key][event.event.CreateEvent.UserId] = true
					unique = 1
				} else {
					usersSet[event.event.CreateEvent.UserId] = true
					unique = len(usersSet)
				}
			case Update:
				// We got a `VisitUpdate`, we just need to output a record with
				// with the updated unique count
				usersSet, ok := state[key]
				if !ok {
					unique = 1
				} else {
					unique = len(usersSet)
				}
			}
			event.summary.Uniques = unique
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func foldSummaries(ctx context.Context, events <-chan PartialRecord) chan DocumentSummary {
	out := make(chan DocumentSummary)
	go func() {
		defer close(out)
		state := make(map[string]DocumentSummary)
		lastVisits := make(map[string]time.Time)
		for event := range events {
			key := fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.DocumentId)
			if len(state) == 0 {
				state[key] = event.summary
				lastVisits[event.summary.DocumentId] = event.instant
			} else {
				summary, ok := state[key]
				if !ok {
					// A new `DocumentSummary` arrived, we want to check if
					// the visit creation time exceeds our threshold of 1 hour and emit
					// the record in case, otherwise we just store the new document in
					// the state
					state[key] = event.summary
					lastVisit, ok := lastVisits[event.summary.DocumentId]
					if !ok {
						lastVisits[event.summary.DocumentId] = summary.Start
					} else {
						if event.instant.Unix()-lastVisit.Unix() > window {
							timerange := getTimeRangeWithin(lastVisit, window)
							rmKey := fmt.Sprintf("%d%s", timerange, event.summary.DocumentId)
							doc, ok := state[rmKey]
							if !ok {
								lastVisits[event.summary.DocumentId] = event.instant
								continue
							}
							delete(state, rmKey)
							lastVisits[event.summary.DocumentId] = event.instant
							select {
							case out <- doc:
							case <-ctx.Done():
							}
						}
					}
				} else {
					summary.Visits += event.summary.Visits
					summary.EngagedTime = event.summary.EngagedTime
					summary.Completion = event.summary.Completion
					summary.Uniques = event.summary.Uniques
					// We want to update the latestDocVisit as it's the most up to
					// date visit received during this time range, as we want to
					// emit the event only after we're sure that no other updates
					// come in for the given document (e.g. when a new document
					// "younger" than at least 1h arrives)
					newKey := fmt.Sprintf("%d%s", summary.Start.Unix(), summary.DocumentId)
					state[newKey] = summary
					lastVisits[summary.DocumentId] = event.instant
				}
			}
		}
		// End, we want to drain the state emitting all possibly remained
		// events
		for _, v := range state {
			out <- v
		}
	}()
	return out
}

func getTimeRangeWithin(instant time.Time, window int64) int64 {
	return (instant.Unix() / window) * window
}

// MergeErrors merges multiple channels of errors.
// Based on https://blog.golang.org/pipelines.
func MergeErrors(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup // We must ensure that the output channel has the capacity to
	// hold as many errors
	// as there are error channels.
	// This will ensure that it never blocks, even
	// if WaitForPipeline returns early.
	out := make(chan error, len(cs)) // Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls
	// wg.Done.
	output := func(c <-chan error) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	} // Start a goroutine to close out once all the output goroutines
	// are done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var errcList []<-chan error
	summaries, errc := summarize(ctx, listEvents(ctx, fileReaderGen(os.Args[1])))
	errcList = append(errcList, errc)
	records := foldSummaries(ctx, uniqueVisits(ctx, updateStats(ctx, summaries)))
	// records, errc2 := updateStats(ctx, summaries)
	// errcList = append(errcList, errc2)
	for record := range records {
		log.Printf("%s\n", record)
	}
	errch := MergeErrors(errcList...)
	for err := range errch {
		if err != nil {
			log.Fatal(err)
		}
	}
}
