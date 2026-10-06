package server_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Mic92/niks3/server/pg"
)

func BenchmarkTouchPresent(b *testing.B) {
	for _, unrelated := range []int{1_000, 20_000} {
		b.Run(fmt.Sprintf("cache=%d", unrelated*2), func(b *testing.B) {
			service, _ := seedCache(b, unrelated)

			defer service.Close()

			lines := strings.Split(strings.TrimSpace(nixosClosure), "\n")
			keys := make([]string, 0, len(lines))

			for _, line := range lines {
				keys = append(keys, strings.Fields(line)[0]+".narinfo")
			}

			_, err := service.Pool.Exec(b.Context(),
				`INSERT INTO closures (updated_at, key) SELECT now(), k FROM unnest($1::varchar[]) AS k`, keys)
			ok(b, err)

			q := pg.New(service.Pool)

			b.ResetTimer()

			for range b.N {
				_, err := q.TouchPresentClosures(b.Context(), keys)
				ok(b, err)
			}
		})
	}
}
