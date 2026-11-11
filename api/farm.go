package api

// LeadRequest is the optional body of POST /api/farm/lead.
type LeadRequest struct {
	// Incumbent: the caller led when its previous stream broke. Right after
	// a server start, others wait briefly so the incumbent keeps the role.
	Incumbent bool `json:"incumbent"`
}

// LeadStatus is one NDJSON line on the /api/farm/lead stream.
type LeadStatus struct {
	Lead bool `json:"lead"`
	// Generation numbers the grants of the lead: every leader is told a
	// higher one than the leader before it, also when it is the same
	// scheduler that comes back. Zero unless Lead.
	Generation uint64 `json:"generation,omitempty"`
}
