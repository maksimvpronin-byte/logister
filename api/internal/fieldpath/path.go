// Package fieldpath resolves paths against arbitrary JSON without imposing a
// schema on field names. JSON Pointer provides unambiguous access to any key.
package fieldpath

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

type step struct {
	key, remaining string
	any            bool
}

func parse(path string) ([]step, error) {
	bad := errors.New("use a field path, array index or JSON Pointer (up to 1024 bytes)")
	if path == "" || len(path) > 1024 || !utf8.ValidString(path) {
		return nil, bad
	}
	if strings.HasPrefix(path, "/") {
		out := []step{}
		for _, token := range strings.Split(path[1:], "/") {
			var key strings.Builder
			for i := 0; i < len(token); i++ {
				if token[i] == '~' {
					i++
					if i >= len(token) || (token[i] != '0' && token[i] != '1') {
						return nil, bad
					}
					if token[i] == '0' {
						key.WriteByte('~')
					} else {
						key.WriteByte('/')
					}
				} else {
					key.WriteByte(token[i])
				}
			}
			out = append(out, step{key: key.String()})
		}
		return out, nil
	}
	if path == "$" {
		return []step{}, nil
	}
	if strings.HasPrefix(path, "$.") {
		path = path[2:]
	} else if strings.HasPrefix(path, "$[") {
		path = path[1:]
	}
	out := []step{}
	for i := 0; i < len(path); {
		if path[i] == '[' {
			i++
			if i < len(path) && path[i] == '"' {
				start := i
				i++
				escaped := false
				for i < len(path) {
					if !escaped && path[i] == '"' {
						break
					}
					if !escaped && path[i] == '\\' {
						escaped = true
					} else {
						escaped = false
					}
					i++
				}
				if i >= len(path) {
					return nil, bad
				}
				var key string
				if json.Unmarshal([]byte(path[start:i+1]), &key) != nil {
					return nil, bad
				}
				i++
				if i >= len(path) || path[i] != ']' {
					return nil, bad
				}
				i++
				out = append(out, step{key: key})
			} else {
				start := i
				for i < len(path) && path[i] != ']' {
					i++
				}
				if i >= len(path) {
					return nil, bad
				}
				key := path[start:i]
				i++
				if key == "" || key == "*" {
					out = append(out, step{any: true})
				} else {
					n, err := strconv.Atoi(key)
					if err != nil || n < 0 {
						return nil, bad
					}
					out = append(out, step{key: key})
				}
			}
		} else {
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				if path[i] == ']' {
					return nil, bad
				}
				i++
			}
			if start == i {
				return nil, bad
			}
			out = append(out, step{key: path[start:i], remaining: path[start:]})
		}
		if i < len(path) && path[i] == '.' {
			i++
			if i >= len(path) || path[i] == '.' || path[i] == '[' {
				return nil, bad
			}
		} else if i < len(path) && path[i] != '[' {
			return nil, bad
		}
	}
	return out, nil
}
func Validate(path string) error { _, err := parse(path); return err }
func Values(root any, path string) ([]any, error) {
	steps, err := parse(path)
	if err != nil {
		return nil, err
	}
	out := []any{}
	var visit func(any, int)
	visit = func(value any, pos int) {
		if pos == len(steps) {
			out = append(out, value)
			return
		}
		s := steps[pos]
		switch current := value.(type) {
		case map[string]any:
			if s.any {
				for _, v := range current {
					visit(v, pos+1)
				}
				return
			}
			if s.remaining != "" {
				if v, ok := current[s.remaining]; ok {
					out = append(out, v)
					return
				}
			}
			if v, ok := current[s.key]; ok {
				visit(v, pos+1)
			}
		case []any:
			if s.any {
				for _, v := range current {
					visit(v, pos+1)
				}
				return
			}
			if index, err := strconv.Atoi(s.key); err == nil && index >= 0 {
				if index < len(current) {
					visit(current[index], pos+1)
				}
				return
			}
			for _, v := range current {
				visit(v, pos)
			}
		}
	}
	visit(root, 0)
	return out, nil
}
