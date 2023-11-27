package shared

import "time"

// cache request
type Request struct {
	Key       string
	Value     string
	Method    string // GET, SET
	Timestamp time.Time
}
