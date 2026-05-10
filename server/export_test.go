package server

import "github.com/Mic92/niks3/api"

// Test-only exports for gcTask methods, callable from server_test package.

func (t *gcTask) TestSucceed(stats api.GCStats)             { t.succeed(stats) }
func (t *gcTask) TestFail(stats api.GCStats, errMsg string) { t.fail(stats, errMsg) }
func (t *gcTask) TestSetPhase(phase api.GCTaskPhase)        { t.setPhase(phase) }
func (t *gcTask) TestUpdateStats(stats api.GCStats)         { t.updateStats(stats) }

// Test-only re-exports for proxy range parsing.

type ByteRange = byteRange

var ErrUnsatisfiableRange = errUnsatisfiableRange

func ParseSingleRange(spec string, size int64) (*ByteRange, error) {
	return parseSingleRange(spec, size)
}

func (br ByteRange) Start() int64 { return br.start }
func (br ByteRange) End() int64   { return br.end }
