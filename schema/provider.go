package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Provider is a source of per-URN data schemas. The reference [MapProvider] is in-memory;
// [DirProvider] reads a babelqueue-registry registry.json. A production provider (a service
// client, an embedded bundle) implements the same single method.
type Provider interface {
	// Schema returns the schema registered for urn. found is false when the URN has no
	// registered schema, in which case the caller skips validation (opt-in).
	Schema(urn string) (sch *Schema, found bool, err error)
}

// MapProvider is an in-memory [Provider], suitable for tests and for embedding schemas in
// code. It is read-only after construction and therefore safe for concurrent use.
type MapProvider struct {
	schemas map[string]*Schema
}

// NewMapProvider builds a MapProvider from URN → raw JSON Schema bytes, parsing each.
func NewMapProvider(raw map[string][]byte) (*MapProvider, error) {
	m := &MapProvider{schemas: make(map[string]*Schema, len(raw))}
	for urn, body := range raw {
		s, err := Parse(body)
		if err != nil {
			return nil, fmt.Errorf("schema: %q: %w", urn, err)
		}
		m.schemas[urn] = s
	}
	return m, nil
}

// Schema implements [Provider].
func (m *MapProvider) Schema(urn string) (*Schema, bool, error) {
	s, ok := m.schemas[urn]
	return s, ok, nil
}

// DirProvider reads schemas from a babelqueue-registry manifest (registry.json): a list of
// {urn, schema} entries mapping each URN to a draft-07 schema file for its data block. This
// is the bridge that makes the registry's governed schemas enforceable at runtime. The
// manifest is read once; schema files are parsed lazily and cached.
type DirProvider struct {
	dir   string
	files map[string]string // urn -> schema file path (relative to dir)

	mu    sync.Mutex
	cache map[string]*Schema
}

type manifest struct {
	Schemas []struct {
		URN    string `json:"urn"`
		Schema string `json:"schema"`
	} `json:"schemas"`
}

// NewDirProvider loads the registry manifest at manifestPath (e.g. ".../registry.json").
func NewDirProvider(manifestPath string) (*DirProvider, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", manifestPath, err)
	}
	var man manifest
	if err := json.Unmarshal(data, &man); err != nil {
		return nil, fmt.Errorf("schema: parse %s: %w", manifestPath, err)
	}
	p := &DirProvider{
		dir:   filepath.Dir(manifestPath),
		files: make(map[string]string, len(man.Schemas)),
		cache: make(map[string]*Schema),
	}
	for _, e := range man.Schemas {
		if e.URN == "" || e.Schema == "" {
			continue
		}
		p.files[e.URN] = e.Schema
	}
	return p, nil
}

// Schema implements [Provider], reading and caching the schema file on first use.
func (p *DirProvider) Schema(urn string) (*Schema, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if s, ok := p.cache[urn]; ok {
		return s, true, nil
	}
	file, ok := p.files[urn]
	if !ok {
		return nil, false, nil
	}
	path := file
	if !filepath.IsAbs(path) {
		path = filepath.Join(p.dir, file)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, true, fmt.Errorf("schema: read schema for %q (%s): %w", urn, file, err)
	}
	s, err := Parse(raw)
	if err != nil {
		return nil, true, fmt.Errorf("schema: %q: %w", urn, err)
	}
	p.cache[urn] = s
	return s, true, nil
}
