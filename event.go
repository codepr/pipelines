package main

import (
	"fmt"
	"time"
)

type EventType string

const (
	Create EventType = "VisitCreate"
	Update EventType = "VisitUpdate"
)

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

type PartialEvent struct {
	summary PageSummary
	event   Event
	instant time.Time
}
