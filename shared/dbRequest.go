package shared

type DbRequest struct {
	Command string // "PUT", "GET", or "DELETE"
	Key     string
	Value   string // Empty for "GET" and "DELETE" operations
}
