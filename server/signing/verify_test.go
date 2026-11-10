package signing_test

import (
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/signing"
)

func TestValidatePublicKeys(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		"no-colon",
		":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"short-test-1:AAAA",
		"bad-test-1:!!!",
	} {
		if err := signing.ValidatePublicKeys([]string{key}); err == nil {
			t.Errorf("ValidatePublicKeys(%q) succeeded", key)
		}
	}

	if err := signing.ValidatePublicKeys([]string{"good-test-1:" + strings.Repeat("A", 43) + "="}); err != nil {
		t.Errorf("valid key: %v", err)
	}
}
