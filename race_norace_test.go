//go:build !race

package babelqueue_test

// raceEnabled reports whether the test binary was built with the race detector
// (`go test -race`). The GR-8 overhead gate is a pure-CPU timing measurement, so it
// is skipped under -race (which instruments every memory access and distorts timing)
// and enforced in the non-race coverage job instead.
const raceEnabled = false
