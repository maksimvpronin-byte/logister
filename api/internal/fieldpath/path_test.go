package fieldpath

import "testing"

func TestArbitraryJSONPaths(t *testing.T) {
	root := map[string]any{"service": "billing", "event": map[string]any{"code": 500}, "a.b": "literal", "a": map[string]any{"b": "nested"}, "fields": map[string]any{"a.b": "old dotted key"}, "items": []any{map[string]any{"code": 1}, map[string]any{"code": 2}}, "random / key": map[string]any{"~value": nil}, "ключ с пробелом": true, "": 3}
	for _, tc := range []struct {
		path  string
		count int
		first any
	}{
		{"service", 1, "billing"}, {"event.code", 1, 500}, {"$.event.code", 1, 500}, {"a.b", 1, "literal"}, {"/a/b", 1, "nested"}, {`["a.b"]`, 1, "literal"},
		{"fields.a.b", 1, "old dotted key"}, {"items[0].code", 1, 1}, {"items[].code", 2, 1}, {"items.code", 2, 1}, {"/items/1/code", 1, 2},
		{"/random ~1 key/~0value", 1, nil}, {`["ключ с пробелом"]`, 1, true}, {"/", 1, 3}, {"missing", 0, nil},
	} {
		values, err := Values(root, tc.path)
		if err != nil || len(values) != tc.count {
			t.Fatalf("%s: %+v %v", tc.path, values, err)
		}
		if tc.count > 0 && values[0] != tc.first {
			t.Fatalf("%s: %+v", tc.path, values)
		}
	}
}
func TestRejectMalformedPaths(t *testing.T) {
	for _, path := range []string{"", "x[", "a..b", "a.", `["unterminated]`, "/bad~2escape", "x[-1]"} {
		if Validate(path) == nil {
			t.Fatal(path)
		}
	}
}
