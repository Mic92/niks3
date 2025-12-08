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

func TestGetClaim(t *testing.T) {
	claims := map[string]any{
		"simple": "value",
		"nested": map[string]any{
			"key": "nested_value",
			"deep": map[string]any{
				"key": "deep_value",
			},
		},
	}

	tests := []struct {
		name    string
		key     string
		want    any
		wantErr bool
	}{
		{"simple key", "simple", "value", false},
		{"nested key", "nested.key", "nested_value", false},
		{"deep nested key", "nested.deep.key", "deep_value", false},
		{"missing key", "missing", nil, true},
		{"missing nested key", "nested.missing", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getClaim(claims, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("getClaim() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("getClaim() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeToStringSlice(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  []string
	}{
		{"string", "foo", []string{"foo"}},
		{"string slice", []string{"foo", "bar"}, []string{"foo", "bar"}},
		{"any slice", []any{"foo", "bar"}, []string{"foo", "bar"}},
		{"number", 123, []string{"123"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeToStringSlice(tt.value)
			if len(got) != len(tt.want) {
				t.Errorf("normalizeToStringSlice() = %v, want %v", got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("normalizeToStringSlice()[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
