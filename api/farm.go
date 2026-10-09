package api

// LeadRequest is the optional body of POST /api/farm/lead and
// POST /api/farm/lead/{farmID}. The latter elects an independent farm;
// the original endpoint retains its lock for existing clients.
type LeadRequest struct {
	// Incumbent: the caller led when its previous stream broke. Right after
	// a server start, others wait briefly so the incumbent keeps the role.
	Incumbent bool `json:"incumbent"`
}

// LeadStatus is one NDJSON line on the /api/farm/lead stream.
type LeadStatus struct {
	Lead bool `json:"lead"`
}
