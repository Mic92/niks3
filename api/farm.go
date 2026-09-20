package api

// LeadStatus is one NDJSON line on the /api/farm/lead stream.
type LeadStatus struct {
	Lead bool `json:"lead"`
}
