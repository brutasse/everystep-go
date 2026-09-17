// Package serde is JSON (de)serialization for workflow data and recorded
// errors.
//
// Extended types are tagged where they appear in dynamic positions (any,
// map, slice); inside struct fields they use their native JSON form
// (RFC 3339, base64, the uuid string), which round-trips on unmarshal:
//
//	time.Time     {"__everystep_type__": "datetime",  "value": RFC 3339Nano}
//	time.Duration {"__everystep_type__": "timedelta", "value": total seconds}
//	uuid.UUID     {"__everystep_type__": "uuid",      "value": "550e..."}
//	[]byte        {"__everystep_type__": "bytes",     "value": base64}
package serde

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

const tag = "__everystep_type__"

// DefaultHook is the encoder default for everystep's extended types. It is
// exposed so host JSON can use the same rules: pass it as the default of a
// json.JSONEncoder.
func DefaultHook(v any) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return tagged("datetime", t.Format(time.RFC3339Nano)), nil
	case time.Duration:
		return tagged("timedelta", t.Seconds()), nil
	case uuid.UUID:
		return tagged("uuid", t.String()), nil
	case []byte:
		return tagged("bytes", base64.StdEncoding.EncodeToString(t)), nil
	}
	return nil, fmt.Errorf(
		"everystep cannot serialize %T; workflow data must be JSON-serializable "+
			"(time.Time, time.Duration, uuid.UUID and []byte are supported)", v)
}

func tagged(kind string, value any) map[string]any {
	return map[string]any{tag: kind, "value": value}
}

// Marshal serializes v to JSON, tagging the extended types.
func Marshal(v any) ([]byte, error) {
	norm, err := normalize(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(norm)
}

// normalize walks the dynamic positions of v, tagging the extended types.
// Structs are left to encoding/json, which encodes their fields natively.
func normalize(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case time.Time:
		return tagged("datetime", t.Format(time.RFC3339Nano)), nil
	case time.Duration:
		return tagged("timedelta", t.Seconds()), nil
	case uuid.UUID:
		return tagged("uuid", t.String()), nil
	case []byte:
		return tagged("bytes", base64.StdEncoding.EncodeToString(t)), nil
	case json.RawMessage:
		return t, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			n, err := normalize(val)
			if err != nil {
				return nil, err
			}
			out[k] = n
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			n, err := normalize(val)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil, nil
		}
		return normalize(rv.Elem().Interface())
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return v, nil
		}
		keys := rv.MapKeys()
		out := make(map[string]any, len(keys))
		for _, k := range keys {
			n, err := normalize(rv.MapIndex(k).Interface())
			if err != nil {
				return nil, err
			}
			out[k.String()] = n
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			n, err := normalize(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	}
	return v, nil
}

// Unmarshal parses data into dest, decoding the tagged extended types.
func Unmarshal(data []byte, dest any) error {
	var tree any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&tree); err != nil {
		return err
	}
	walked, err := detag(tree)
	if err != nil {
		return err
	}
	if dest == nil {
		return nil
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("serde: dest must be a non-nil pointer, got %T", dest)
	}
	e := rv.Elem()
	// Dynamic destinations take the walked tree directly so the decoded
	// extended types survive; typed destinations go through a re-marshal.
	switch {
	case e.Kind() == reflect.Interface:
		e.Set(reflect.ValueOf(walked))
		return nil
	case e.Type() == anyMapType && reflect.TypeOf(walked).Kind() == reflect.Map:
		e.Set(reflect.ValueOf(walked))
		return nil
	case e.Type() == anySliceType && reflect.TypeOf(walked).Kind() == reflect.Slice:
		e.Set(reflect.ValueOf(walked))
		return nil
	}
	raw, err := json.Marshal(walked)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dest)
}

var (
	anyMapType   = reflect.TypeOf(map[string]any(nil))
	anySliceType = reflect.TypeOf([]any(nil))
)

// detag rebuilds the Go values from a generic JSON tree.
func detag(tree any) (any, error) {
	switch v := tree.(type) {
	case map[string]any:
		if len(v) == 2 {
			if kind, ok := v[tag].(string); ok {
				if val, present := v["value"]; present {
					if dec, ok := detaggers[kind]; ok {
						return dec(val)
					}
				}
			}
		}
		out := make(map[string]any, len(v))
		for k, val := range v {
			n, err := detag(val)
			if err != nil {
				return nil, err
			}
			out[k] = n
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			n, err := detag(val)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case json.Number:
		return numberValue(v)
	}
	return tree, nil
}

var detaggers = map[string]func(any) (any, error){
	"datetime": func(val any) (any, error) {
		s, _ := val.(string)
		return time.Parse(time.RFC3339Nano, s)
	},
	"timedelta": func(val any) (any, error) {
		f, ok := numberAsFloat(val)
		if !ok {
			return nil, fmt.Errorf("serde: invalid timedelta value %v", val)
		}
		return time.Duration(f * float64(time.Second)), nil
	},
	"uuid": func(val any) (any, error) {
		s, _ := val.(string)
		return uuid.Parse(s)
	},
	"bytes": func(val any) (any, error) {
		s, _ := val.(string)
		return base64.StdEncoding.DecodeString(s)
	},
}

func numberAsFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

func numberValue(n json.Number) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	// Beyond int64 range: keep the number so it re-marshals verbatim.
	return n, nil
}
