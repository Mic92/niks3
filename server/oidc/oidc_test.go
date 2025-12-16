package oidc

import (
	"testing"
)

func TestGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		// Exact matches
		{"foo", "foo", true},
		{"foo", "bar", false},

		// Star wildcard
		{"*", "", true},
		{"*", "anything", true},
		{"foo*", "foo", true},
		{"foo*", "foobar", true},
		{"foo*", "bar", false},
		{"*bar", "bar", true},
		{"*bar", "foobar", true},
		{"*bar", "foo", false},
		{"foo*bar", "foobar", true},
		{"foo*bar", "foo123bar", true},
		{"foo*bar", "foobarbaz", false},

		// Multiple stars
		{"*/*", "foo/bar", true},
		{"*/*", "foo", false},
		{"refs/heads/*", "refs/heads/main", true},
		{"refs/heads/*", "refs/tags/v1.0", false},
		{"refs/*/main", "refs/heads/main", true},

		// Question mark
		{"fo?", "foo", true},
		{"fo?", "fo", false},
		{"fo?", "fooo", false},
		{"?oo", "foo", true},
		{"?oo", "boo", true},

		// GitHub Actions patterns
		{"repo:myorg/*:*", "repo:myorg/myrepo:ref:refs/heads/main", true},
		{"repo:myorg/*:*", "repo:otherorg/myrepo:ref:refs/heads/main", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.value, func(t *testing.T) {
			if got := globMatch(tt.pattern, tt.value); got != tt.want {
				t.Errorf("globMatch(%q, %q) = %v, want %v", tt.pattern, tt.value, got, tt.want)
			}
		})
	}
}

func TestValidateBoundClaims(t *testing.T) {
	tests := []struct {
		name        string
		claims      map[string]any
		boundClaims map[string][]string
		wantErr     bool
	}{
		{
			name: "exact match",
			claims: map[string]any{
				"repository_owner": "myorg",
			},
			boundClaims: map[string][]string{
				"repository_owner": {"myorg"},
			},
			wantErr: false,
		},
		{
			name: "glob match",
			claims: map[string]any{
				"ref": "refs/heads/main",
			},
			boundClaims: map[string][]string{
				"ref": {"refs/heads/*"},
			},
			wantErr: false,
		},
		{
			name: "multiple allowed values",
			claims: map[string]any{
				"ref": "refs/tags/v1.0",
			},
			boundClaims: map[string][]string{
				"ref": {"refs/heads/main", "refs/tags/*"},
			},
			wantErr: false,
		},
		{
			name: "multiple bound claims all must match",
			claims: map[string]any{
				"repository_owner": "myorg",
				"ref":              "refs/heads/main",
			},
			boundClaims: map[string][]string{
				"repository_owner": {"myorg"},
				"ref":              {"refs/heads/*"},
			},
			wantErr: false,
		},
		{
			name: "claim not found",
			claims: map[string]any{
				"other": "value",
			},
			boundClaims: map[string][]string{
				"repository_owner": {"myorg"},
			},
			wantErr: true,
		},
		{
			name: "claim value not allowed",
			claims: map[string]any{
				"repository_owner": "otherorg",
			},
			boundClaims: map[string][]string{
				"repository_owner": {"myorg"},
			},
			wantErr: true,
		},
		{
			name: "one of multiple bound claims fails",
			claims: map[string]any{
				"repository_owner": "myorg",
				"ref":              "refs/heads/feature",
			},
			boundClaims: map[string][]string{
				"repository_owner": {"myorg"},
				"ref":              {"refs/heads/main"},
			},
			wantErr: true,
		},
		{
			name:        "empty bound claims always passes",
			claims:      map[string]any{"anything": "value"},
			boundClaims: map[string][]string{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBoundClaims(tt.claims, tt.boundClaims)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateBoundClaims() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateBoundSubject(t *testing.T) {
	tests := []struct {
		name         string
		claims       map[string]any
		boundSubject []string
		wantErr      bool
	}{
		{
			name:         "empty bound subject passes",
			claims:       map[string]any{"sub": "anything"},
			boundSubject: []string{},
			wantErr:      false,
		},
		{
			name:         "exact match",
			claims:       map[string]any{"sub": "repo:myorg/myrepo:ref:refs/heads/main"},
			boundSubject: []string{"repo:myorg/myrepo:ref:refs/heads/main"},
			wantErr:      false,
		},
		{
			name:         "glob match",
			claims:       map[string]any{"sub": "repo:myorg/myrepo:ref:refs/heads/main"},
			boundSubject: []string{"repo:myorg/*:*"},
			wantErr:      false,
		},
		{
			name:         "no match",
			claims:       map[string]any{"sub": "repo:otherorg/myrepo:ref:refs/heads/main"},
			boundSubject: []string{"repo:myorg/*:*"},
			wantErr:      true,
		},
		{
			name:         "missing sub claim",
			claims:       map[string]any{"other": "value"},
			boundSubject: []string{"something"},
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBoundSubject(tt.claims, tt.boundSubject)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateBoundSubject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
