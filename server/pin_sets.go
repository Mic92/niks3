package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// A host reads its closure in one burst, then not for hours. A set
	// loaded again also has what later pushes added to its closure.
	pinSetLifetime = 10 * time.Minute
	// About 32 MB. Only many hosts on distinct systems, reading at the
	// same time, get here.
	pinSetMaxKeys = 2_000_000
)

// pinSets remembers what each pin target reaches. Walking a closure for
// every request of a substitution would be too slow.
type pinSets struct {
	mu      sync.Mutex
	sets    map[string]*pinSet
	keys    int
	loading singleflight.Group

	keyed sync.Once
	mac   [sha256.Size]byte
}

// pinKey stands for an object key at a sixth of its size. It is a keyed
// cryptographic hash, so that nobody can make up a key that passes for
// another.
type pinKey [16]byte

func (c *pinSets) digest(key string) pinKey {
	c.keyed.Do(func() { _, _ = rand.Read(c.mac[:]) })

	h := hmac.New(sha256.New, c.mac[:])
	h.Write([]byte(key))

	var digest pinKey

	copy(digest[:], h.Sum(nil))

	return digest
}

type pinSet struct {
	keys   []pinKey // sorted
	loaded time.Time
	used   time.Time
}

func (s *pinSet) has(key pinKey) bool {
	_, found := slices.BinarySearchFunc(s.keys, key, comparePinKeys)

	return found
}

func comparePinKeys(a, b pinKey) int { return bytes.Compare(a[:], b[:]) }

// reaches reports whether key is in the closure of target. load walks the
// closure when no fresh set is at hand.
func (c *pinSets) reaches(
	ctx context.Context, target, key string, load func(context.Context, string) ([]string, error),
) (bool, error) {
	digest := c.digest(key)

	if set := c.fresh(target); set != nil {
		return set.has(digest), nil
	}

	// Requests that arrive during the walk wait for it, so it must outlive
	// the one that started it.
	loaded, err, _ := c.loading.Do(target, func() (any, error) {
		keys, err := load(context.WithoutCancel(ctx), target)
		if err != nil {
			return nil, err
		}

		set := &pinSet{keys: make([]pinKey, len(keys)), loaded: time.Now(), used: time.Now()}
		for i, k := range keys {
			set.keys[i] = c.digest(k)
		}

		slices.SortFunc(set.keys, comparePinKeys)

		c.keep(target, set)

		return set, nil
	})
	if err != nil {
		return false, err //nolint:wrapcheck // the caller names the target
	}

	set, _ := loaded.(*pinSet)

	return set.has(digest), nil
}

func (c *pinSets) fresh(target string) *pinSet {
	c.mu.Lock()
	defer c.mu.Unlock()

	set := c.sets[target]
	if set == nil || time.Since(set.loaded) >= pinSetLifetime {
		return nil
	}

	set.used = time.Now()

	return set
}

func (c *pinSets) keep(target string, set *pinSet) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sets == nil {
		c.sets = map[string]*pinSet{}
	}

	if len(c.sets) == 0 {
		time.AfterFunc(pinSetLifetime, c.dropOld)
	}

	c.drop(target)
	c.sets[target] = set
	c.keys += len(set.keys)

	for c.keys > pinSetMaxKeys && len(c.sets) > 1 {
		oldest := ""
		for name, other := range c.sets {
			if name != target && (oldest == "" || other.used.Before(c.sets[oldest].used)) {
				oldest = name
			}
		}

		c.drop(oldest)
	}
}

func (c *pinSets) drop(target string) {
	if set := c.sets[target]; set != nil {
		c.keys -= len(set.keys)
		delete(c.sets, target)
	}
}

func (c *pinSets) dropOld() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for target, set := range c.sets {
		if time.Since(set.loaded) >= pinSetLifetime {
			c.drop(target)
		}
	}

	if len(c.sets) > 0 {
		time.AfterFunc(pinSetLifetime, c.dropOld)
	}
}
