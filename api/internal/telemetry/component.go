package telemetry

type Component struct {
	Status  string             `json:"status"`
	Metrics map[string]float64 `json:"metrics"`
	Note    string             `json:"note,omitempty"`
}
