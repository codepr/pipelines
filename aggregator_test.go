package main

import (
	"context"
	"testing"
	"time"
)

var batchone = []string{
	`{"eventType":"VisitCreate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f24","userId":"630adcc1-e302-40a6-8af2-d2cd84e71721","pageId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-18T23:55:49.254Z"}`,
	`{"eventType":"VisitCreate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f25","userId":"630adcc1-e302-40a6-8af2-d2cd84e71720","pageId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-18T23:55:49.254Z"}`,
	`{"eventType":"VisitCreate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","userId":"630adcc1-e302-40a6-8af2-d2cd84e71720","pageId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-18T23:55:49.254Z"}`,
	`{"eventType":"VisitUpdate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","engagedTime":300,"completion":0.2,"updatedAt":"2015-05-18T23:56:54.357Z"}`,
	`{"eventType":"VisitUpdate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","engagedTime":600,"completion":0.4,"updatedAt":"2015-05-19T00:50:55.357Z"}`,
	`{"eventType":"VisitUpdate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","engagedTime":900,"completion":1,"updatedAt":"2015-05-19T00:51:54.357Z"}`,
	`{"eventType":"VisitCreate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f27","userId":"630adcc1-e302-40a6-8af2-d2cd84e71722","pageId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-19T00:55:49.254Z"}`,
	`{"eventType":"VisitCreate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f28","userId":"630adcc1-e302-40a6-8af2-d2cd84e71722","pageId":"b61d8914-560f-4985-8a5f-aa974ad0c7ac","createdAt":"2015-05-19T01:55:50.254Z"}`,
	`{"eventType":"VisitUpdate","id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f28","engagedTime":1800,"completion":0.2,"updatedAt":"2015-05-19T01:55:51.254Z"}`,
}

type sliceEventSource struct {
	slice []string
}

func (s sliceEventSource) Run() (<-chan []byte, <-chan error) {
	out := make(chan []byte)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for _, line := range s.slice {
			out <- []byte(line)
		}
	}()
	return out, errc
}

func TestSummarizeBatchOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := sliceEventSource{batchone}
	aggregator := &VisitEventAggregator{1 * time.Hour, 64, source}
	c, errc := aggregator.Summarize(ctx)
	var (
		results []PageSummary
		errors  []error
	)
	for record := range c {
		results = append(results, record)
	}
	for err := range errc {
		errors = append(errors, err)
	}
	if len(errors) != 0 {
		t.Errorf("summarize batchone failed, expected no errors, got: %#v", errors)
	}
	if len(results) != 5 {
		t.Errorf("summarize batchone failed, expected 3 summaries, got %d", len(results))
	}
}
