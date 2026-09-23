package server_test

import (
	_ "embed"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
)

// Reference graph of a NixOS system closure: one line per store path, the
// path's hash followed by the hashes it references. The first line is the root.
//
//go:embed testdata/nixos-closure.txt
var nixosClosure string

// seedCache stores the NixOS closure as live objects (a narinfo and a NAR per
// path) next to unrelatedPaths more live paths. It returns the closure's root.
func seedCache(b *testing.B, unrelatedPaths int) (*server.Service, string) {
	b.Helper()

	service := createTestService(b)

	lines := strings.Split(strings.TrimSpace(nixosClosure), "\n")
	keys := make([]string, 0, 2*len(lines))
	refs := make([]string, 0, 2*len(lines))

	var root string

	for i, line := range lines {
		fields := strings.Fields(line)
		hash := fields[0]

		if i == 0 {
			root = hash + ".narinfo"
		}

		narinfoRefs := []string{narKeyFor(hash)}
		for _, r := range fields[1:] {
			narinfoRefs = append(narinfoRefs, r+".narinfo")
		}

		keys = append(keys, hash+".narinfo", narKeyFor(hash))
		refs = append(refs, "{"+strings.Join(narinfoRefs, ",")+"}", "{}")
	}

	_, err := service.Pool.Exec(b.Context(),
		"INSERT INTO objects (key, refs) SELECT k, r::varchar[] FROM unnest($1::varchar[], $2::text[]) AS t(k, r)",
		keys, refs)
	ok(b, err)

	_, err = service.Pool.Exec(b.Context(), `
		INSERT INTO objects (key, refs)
		SELECT md5('u' || i) || '.narinfo', ARRAY['nar/' || md5('u' || i) || '.nar.zst']
		FROM generate_series(1, $1::int) AS i
		UNION ALL
		SELECT 'nar/' || md5('u' || i) || '.nar.zst', '{}'
		FROM generate_series(1, $1::int) AS i`, unrelatedPaths)
	ok(b, err)

	_, err = service.Pool.Exec(b.Context(), "ANALYZE objects")
	ok(b, err)

	return service, root
}

// BenchmarkCommitPush times completing a push whose whole closure is already
// live, as for a rebuilt NixOS system.
func BenchmarkCommitPush(b *testing.B) {
	for _, unrelated := range []int{1_000, 20_000, 95_000} {
		b.Run(fmt.Sprintf("cache=%d", unrelated*2), func(b *testing.B) {
			service, root := seedCache(b, unrelated)

			defer service.Close()

			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				var id int64

				ok(b, service.Pool.QueryRow(b.Context(),
					`INSERT INTO pending_closures (key, started_at, roots)
					 VALUES ($1::varchar, $2, ARRAY[$1::varchar]) RETURNING id`, root, time.Now().UTC()).Scan(&id))

				b.StartTimer()

				_, err := service.Pool.Exec(b.Context(), "SELECT commit_push($1)", id)
				ok(b, err)
			}
		})
	}
}
