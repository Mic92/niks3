// Package testwait holds the one bound tests use for things that must happen.
package testwait

import "time"

// Hung is how long a test waits before it calls something stuck. It only
// tells a hang from progress, so it is far above what a starved builder
// needs: bounds of five seconds failed there on work that takes milliseconds.
const Hung = 2 * time.Minute
