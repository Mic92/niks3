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
}
