package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMapProvider(t *testing.T) {
	p, err := NewMapProvider(map[string][]byte{
		"urn:babel:orders:created": []byte(`{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	s, found, err := p.Schema("urn:babel:orders:created")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if errs := s.Validate(map[string]any{"order_id": 1.0}); len(errs) != 0 {
		t.Fatalf("valid payload rejected: %v", errs)
	}
	if _, found, _ := p.Schema("urn:babel:unknown"); found {
		t.Fatal("an unregistered urn should be not-found")
	}
}

func TestNewMapProvider_BadSchema(t *testing.T) {
	if _, err := NewMapProvider(map[string][]byte{"u": []byte("not json")}); err == nil {
		t.Fatal("a bad schema should error")
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDirProvider_LazyLoadAndCache(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "schemas/orders-created.json"),
		`{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}}}`)
	// the empty-urn entry is ignored on load
	writeFile(t, filepath.Join(dir, "registry.json"),
		`{"schemas":[{"urn":"urn:babel:orders:created","schema":"schemas/orders-created.json"},{"urn":"","schema":"x"}]}`)

	p, err := NewDirProvider(filepath.Join(dir, "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // second call hits the cache
		s, found, err := p.Schema("urn:babel:orders:created")
		if err != nil || !found {
			t.Fatalf("found=%v err=%v", found, err)
		}
		if errs := s.Validate(map[string]any{"order_id": 1.0}); len(errs) != 0 {
			t.Fatalf("valid payload rejected: %v", errs)
		}
	}
	if _, found, _ := p.Schema("urn:babel:unknown"); found {
		t.Fatal("an unregistered urn should be not-found")
	}
}

func TestDirProvider_Errors(t *testing.T) {
	if _, err := NewDirProvider(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("a missing manifest should error")
	}

	bad := t.TempDir()
	writeFile(t, filepath.Join(bad, "registry.json"), `not json`)
	if _, err := NewDirProvider(filepath.Join(bad, "registry.json")); err == nil {
		t.Fatal("an invalid manifest should error")
	}

	missing := t.TempDir()
	writeFile(t, filepath.Join(missing, "registry.json"), `{"schemas":[{"urn":"u","schema":"missing.json"}]}`)
	p, err := NewDirProvider(filepath.Join(missing, "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := p.Schema("u"); !found || err == nil {
		t.Fatalf("a missing schema file should report found=true with an error; got found=%v err=%v", found, err)
	}

	invalid := t.TempDir()
	writeFile(t, filepath.Join(invalid, "bad.json"), `not json`)
	writeFile(t, filepath.Join(invalid, "registry.json"), `{"schemas":[{"urn":"u","schema":"bad.json"}]}`)
	p2, _ := NewDirProvider(filepath.Join(invalid, "registry.json"))
	if _, found, err := p2.Schema("u"); !found || err == nil {
		t.Fatalf("an invalid schema file should report found=true with an error; got found=%v err=%v", found, err)
	}
}
