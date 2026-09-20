package api

// PresentRequest asks which narinfo keys are cached.
type PresentRequest struct {
	Keys []string `json:"keys"`
}

type PresentResponse struct {
	Present []string `json:"present"`
}
