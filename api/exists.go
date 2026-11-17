package api

// ExistsRequest asks which narinfo keys the cache has.
type ExistsRequest struct {
	Keys []string `json:"keys"`
}

type ExistsResponse struct {
	Exists []string `json:"exists"`
}

// PresentRequest is the request of the deprecated /api/objects/present.
type PresentRequest = ExistsRequest

// PresentResponse is the answer of the deprecated /api/objects/present.
type PresentResponse struct {
	Present []string `json:"present"`
}
