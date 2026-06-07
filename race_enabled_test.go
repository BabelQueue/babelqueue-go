//go:build race

package babelqueue_test

// raceEnabled reports whether the test binary was built with the race detector
// (`go test -race`). See race_norace_test.go for why the GR-8 gate consults it.
const raceEnabled = true
