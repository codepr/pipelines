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
	Id        string    `json:"id"`
	UserId    string    `json:"userId"`
	PageId    string    `json:"pageId"`
	CreatedAt time.Time `json:"createdAt"`
}

type UpdateEvent struct {
	Id          string    `json:"id"`
	EngagedTime int       `json:"engagedTime"`
	Completion  float64   `json:"completion"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Type struct {
	EventType EventType
}

type Event struct {
	Type
	*CreateEvent
	*UpdateEvent
}

type PageSummary struct {
	PageId      string
	Start       time.Time
	End         time.Time
	Hits        int
	Uniques     int
	EngagedTime float64
	Completion  int
}

func (d PageSummary) String() string {
	return fmt.Sprintf("%s,%s,%s,%d,%d,%f,%d", d.PageId, d.Start, d.End, d.Hits, d.Uniques, d.EngagedTime, d.Completion)
}

type PartialRecord struct {
	summary PageSummary
	event   Event
	instant time.Time
}

func (e *Event) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &e.Type); err != nil {
		return err
	}
	switch e.Type.EventType {
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
		return fmt.Errorf("unrecognized type value %q", e.Type.EventType)
	}
	return nil
}

type EventSource interface {
	Run() (<-chan []byte, <-chan error)
}

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

type EventResult struct {
	event Event
	err   error
}

func unmarshalEvents(ctx context.Context, lines <-chan []byte) <-chan EventResult {
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

// mapToPage separates creation from update events and act accordingly, in case of
// `VisitCreate` create a new `PageSummary`, output it and store it
// as state; in case of `VisitUpdate` ensure that a `VisitCreate` exists
// in the past and that it happened not earlier than an hour, then it
// updates the associated `PageSummary`, in case any of the 2
// constraints are not satisfied the `VisitUpdate` event will be dropped.
//
// Results in a stream of (`Visit`, `Instant`, `PageSummary`) tuples, Visit is the
// same input record unchanged, PageSummary is the most up-to-date
// summary for every pageId.
// The Instant is the latest visit creation time for the page. We want to retain
// it as it's used later downstream to decide if a `PageSummary` has to be
// considered completed or if we want to wait for more updates, as updates are
// tracked up to 1 hour after the last visit, we are reasobably sure that if we
// receive a page visited more than an hour after the latest visit for the
// same ID, we can emit it.
func mapToPage(ctx context.Context, events <-chan EventResult) <-chan PartialRecord {
	type tuple struct {
		time    time.Time
		summary PageSummary
	}
	out := make(chan PartialRecord)
	// Inner goroutine make the function acting as a generator, carrying a state using a Map:
	//
	// - state (VisitId -> (Instant, PageSummary)), key is the visitId,
	//   mapping to the `PageSummary` it refers to, along with the `createdAt`
	//   Instant indicating the moment the visit happened. The `PageSummary`
	//   will be created with `start` and `end` members already set to the hour
	//   the visit "belongs to".
	//
	//   Ex:
	//   VisitCreate `a` at 14:42:42:000Z belongs to interval
	//   14:00:00:000Z - 15:00:00:000Z and thus the mapping will be
	//   a -> (14:42:42:000Z, PageSummary(start = 14:00:00:000Z, end = 15:00:00:000Z))
	//
	//   This is done for 2 reasons,
	//      - track that every `VisitUpdate` have a previous `VisitCreate` to
	//        refer to, otherwise we can't infer the page it refers to,
	//        effectively discarding spurious `VisitUpdate` events
	//      - verify that each `VisitUpdate` we receive happened within an hour
	//        from the paired `VisitCreate`, discarding every `VisitUpdate`
	//        breaking this rule.
	go func() {
		defer close(out)
		state := make(map[string]tuple)
		for event := range events {
			if event.err != nil {
				continue
			}
			switch event.event.Type.EventType {
			// We got a VisitCreate event, we want to count it as new visit,
			// transforming it into a new PageSummary event
			case Create:
				startTime := time.Unix((event.event.CreatedAt.Unix()/window)*window, 0)
				summary := PageSummary{
					PageId:      event.event.PageId,
					Start:       startTime,
					End:         startTime.Add(1 * time.Hour),
					Hits:        1,
					Uniques:     1,
					EngagedTime: 0.0,
					Completion:  0,
				}
				state[event.event.CreateEvent.Id] = tuple{event.event.CreatedAt, summary}
				out <- PartialRecord{summary, event.event, event.event.CreatedAt}
			// We got a VisitUpdate event, we need to check 2 things:
			// - It's subsequent to a VisitCreate happened somewhere in the past
			// - It happened within an hour of that VisitCreate event
			case Update:
				tupleState, ok := state[event.event.UpdateEvent.Id]
				// No previous `VisitCreate` happened, spurious `VisitUpdate` here,
				if !ok {
					continue
				}
				// 1 hour window check
				if event.event.UpdateEvent.UpdatedAt.Unix()-tupleState.time.Unix() < window {
					// 1 hour within constraint satisfied, we need to update the
					// associated PageSummary page
					//
					// doc represents the page updated with the stats from the
					// `VisitUpdate`
					tupleState.summary.Hits = 0 // We don't count updates as visits
					tupleState.summary.EngagedTime = float64(event.event.UpdateEvent.EngagedTime)
					tupleState.summary.Completion = int(event.event.UpdateEvent.Completion)
					// We update the visitId -> (createdAt, doc) value in the docs map
					out <- PartialRecord{tupleState.summary, event.event, tupleState.time}
					state[event.event.UpdateEvent.Id] = tupleState // redundant
				} else {
					// 1 hour within constraint not satisfied, we discard the event
					delete(state, event.event.UpdateEvent.Id)
				}
			}
		}
	}()
	return out
}

// updateStats update the total count of each `PageSummary` engagedTime and completions.
//
// Pulls from a stream of `(Visit, Instant, PageSummary)` tuples, taking into consideration
// only `VisitUpdate` events to emit the most up-to-date `PageSummary` records.
//
// The resulting stream output `(Visit, Instant, PageSummary)` events,  they don't need to
// be summed up, every new event store the most up-to-date value regarding
// the number of unique visitors.
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
	// Inner goroutine, makes this function behave like a generator, keeping 2 states using Map as data structure:
	// - state that tracks the most-up-to-date `pageStats` (engagedTime and completion)
	//   for every `PageSummary`
	// - visitStats keeps the most up-to-date `VisitStats` (which expands to (Int, Double)
	//   as (engagedTime, completion)) for every `VisitId`
	//
	// This way it's simple to update the `PageSummary` with only the delta udpates
	// coming from `VisitUpdate` events
	go func() {
		defer close(out)
		state := make(map[string]docstats)
		visits := make(map[string]visitstats)
		for event := range events {
			switch event.event.Type.EventType {
			case Create:
				// A `VisitCreate`, we just pass forward, emitting it into the output stream
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
				dstatsKey := fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.PageId)
				dstats, ok := state[dstatsKey]
				if !ok {
					dstats = docstats{0, 0}
				}
				totalEngagedTime := dstats.engagedTime + (event.event.EngagedTime - vstats.engagedTime)
				totalCompletion := dstats.completion + int(event.event.Completion)
				// Then we update the states for the next round
				state[dstatsKey] = docstats{totalEngagedTime, totalCompletion}
				visits[event.event.UpdateEvent.Id] = visitstats{event.event.EngagedTime, event.event.Completion}
				//finally we emit the updated `PageSummary` event
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

// uniqueVisits count every unique user for each `PageSummary`.
//
//  Requires a stream of `(Visit, Instant, PageSummary)` tuples, count
//  every unique visitor by extracting `userId` of `VisitCreate` case classes
//  and storing it to a `Set`, updating the `uniques` member of the
//  `PageSummary` case class with the cardinality of the associated set of
//  UserIds.
//
//  The resulting stream output (`Instant`, `PageSummary`) events, they
//  don't need to be summed up, every new event store the most up-to-date value
//  regarding the number of unique visitors.
func uniqueVisits(ctx context.Context, events <-chan PartialRecord) <-chan PartialRecord {
	out := make(chan PartialRecord)
	// Inner goroutine, makes the function behave like a generator, tracks
	// unique visitors by storing each `UserId` in an (Instant, pageId)
	// keyed map, updating every event record with the most up-to-date
	// cardinality of the corresponding (Hour, page) pair.
	go func() {
		defer close(out)
		state := make(map[string]map[string]bool)
		for event := range events {
			var (
				unique = 0
				key    = fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.PageId)
			)
			switch event.event.Type.EventType {
			case Create:
				// We got a `VisitCreate`, we want to count every unique `UserId`
				// till now, 1 if it's a first time visit for the page
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

// foldSummaries accumulate each `PageSummary` event record producing the
// aggregated final `PageSummary` for every pageId.
//
// The input stream is formed by tuple (`Instant`, `PageSummary`) events,
// each one representing the visit creation time associated with the latest
// up-to-date page in its time range.
//
// The Instant is the latest visit creation time for the page. We want to
// retain it as it's used later downstream to decide if a `PageSummary` has
// to be considered completed or if we want to wait for more updates, as
// updates are tracked up to 1 hour after the last visit, we are reasonably
// sure that if we receive a page visited more than an hour after the
// latest visit for the same ID, we can emit it.
//
// The output stream is formed by the most up-to-date aggregated
// `PageSummary`
func foldSummaries(ctx context.Context, events <-chan PartialRecord) chan PageSummary {
	out := make(chan PageSummary)
	// Inner goroutine, makes the function behave like a generator, carrying a
	// state, each pageId in a given time-range mapped to a
	// `PageSummary`, updated with every newly received record. An
	// auxiliary map is used to track the updated on the creation date of each
	// page.
	go func() {
		defer close(out)
		state := make(map[string]PageSummary)
		lastVisits := make(map[string]time.Time)
		for event := range events {
			key := fmt.Sprintf("%d%s", event.summary.Start.Unix(), event.summary.PageId)
			// If no pages are stored yet, we just update the docs state with
			// the latest arrived `PageSummary`, we update the latestDocVisit
			// map as well with the creation time of the current page
			if len(state) == 0 {
				state[key] = event.summary
				lastVisits[event.summary.PageId] = event.instant
			} else {
				summary, ok := state[key]
				if !ok {
					// A new `PageSummary` arrived, we want to check if
					// the visit creation time exceeds our threshold of 1 hour and emit
					// the record in case, otherwise we just store the new page in
					// the state
					state[key] = event.summary
					lastVisit, ok := lastVisits[event.summary.PageId]
					if !ok {
						lastVisits[event.summary.PageId] = summary.Start
					} else {
						if event.instant.Unix()-lastVisit.Unix() > window {
							timerange := getTimeRangeWithin(lastVisit, window)
							rmKey := fmt.Sprintf("%d%s", timerange, event.summary.PageId)
							doc, ok := state[rmKey]
							if !ok {
								lastVisits[event.summary.PageId] = event.instant
								continue
							}
							delete(state, rmKey)
							lastVisits[event.summary.PageId] = event.instant
							select {
							case out <- doc:
							case <-ctx.Done():
							}
						}
					}
				} else {
					summary.Hits += event.summary.Hits
					summary.EngagedTime = event.summary.EngagedTime
					summary.Completion = event.summary.Completion
					summary.Uniques = event.summary.Uniques
					// We want to update the latestDocVisit as it's the most up to
					// date visit received during this time range, as we want to
					// emit the event only after we're sure that no other updates
					// come in for the given page (e.g. when a new page
					// "younger" than at least 1h arrives)
					newKey := fmt.Sprintf("%d%s", summary.Start.Unix(), summary.PageId)
					state[newKey] = summary
					lastVisits[summary.PageId] = event.instant
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

// getTimeRangeWithin simple auxiliary function to retrieve the time range an
// instant belongs to based on a fixed time window
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
	source := FileEventSource{os.Args[1]}
	src, errc := source.Run()
	errcList = append(errcList, errc)
	records := foldSummaries(ctx, uniqueVisits(ctx, updateStats(ctx, mapToPage(ctx, unmarshalEvents(ctx, src)))))
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
