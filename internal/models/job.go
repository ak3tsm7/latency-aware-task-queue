package models

type Job struct {
	ID        string                 `json:"job_id"`
	TaskType  string                 `json:"task_type"`
	Requires  string                 `json:"requires"` // cpu | gpu | any
	Priority  int                    `json:"priority"`
	Payload   map[string]interface{} `json:"payload"`
	TimeoutMs int                    `json:"timeout_ms"`
	Metadata  map[string]string      `json:"metadata"`
}
