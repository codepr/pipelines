package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	Create EventType = "VisitCreate"
	Update EventType = "VisitUpdate"
)

// CreateEvent represents a first time visit from an use into a page, just
// tracking its timestamp
type CreateEvent struct {
	Id        string    `json:"id"`
	UserId    string    `json:"userId"`
	PageId    string    `json:"pageId"`
	CreatedAt time.Time `json:"createdAt"`
}

// UpdateEvent represents subsequent visits to a page, keeping track of
// the timestamp of the update, the engaged time and completion expressed as a
// floating point percentage between 0 and 1
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

// PartialEvent is used to keep track of summaries and visit events between
// different downstream steps during the pipeline computation.
type PartialEvent struct {
	summary PageSummary
	event   Event
	instant time.Time
}
