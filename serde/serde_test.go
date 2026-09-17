package serde_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/brutasse/everystep-go/internal/errors"
	"github.com/brutasse/everystep-go/serde"
)

var testUUID = uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

func TestTaggedRoundtrip(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"datetime", when, `{"__everystep_type__":"datetime","value":"2026-01-02T03:04:05.123456Z"}`},
		{"timedelta", 90500 * time.Millisecond, `{"__everystep_type__":"timedelta","value":90.5}`},
		{"uuid", testUUID, `{"__everystep_type__":"uuid","value":"550e8400-e29b-41d4-a716-446655440000"}`},
		{"bytes", []byte{0, 1, 2}, `{"__everystep_type__":"bytes","value":"AAEC"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := serde.Marshal(c.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(raw) != c.want {
				t.Fatalf("Marshal = %s, want %s", raw, c.want)
			}
			var out any
			if err := serde.Unmarshal(raw, &out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			got, ok := out.(any)
			if !ok {
				t.Fatalf("Unmarshal type %T", out)
			}
			switch c.name {
			case "datetime":
				if !got.(time.Time).Equal(c.in.(time.Time)) {
					t.Fatalf("got %v", got)
				}
			case "timedelta":
				if got.(time.Duration) != c.in.(time.Duration) {
					t.Fatalf("got %v", got)
				}
			case "uuid":
				if got.(uuid.UUID) != c.in.(uuid.UUID) {
					t.Fatalf("got %v", got)
				}
			case "bytes":
				if string(got.([]byte)) != string(c.in.([]byte)) {
					t.Fatalf("got %v", got)
				}
			}
		})
	}
}

func TestNestedTagged(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := map[string]any{
		"when": when,
		"list": []any{time.Second, []byte{7}},
		"n":    1,
	}
	raw, err := serde.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := serde.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !out["when"].(time.Time).Equal(when) {
		t.Fatalf("when = %v", out["when"])
	}
	list := out["list"].([]any)
	if list[0].(time.Duration) != time.Second {
		t.Fatalf("list[0] = %v", list[0])
	}
	if string(list[1].([]byte)) != "\x07" {
		t.Fatalf("list[1] = %v", list[1])
	}
	if out["n"].(int64) != 1 {
		t.Fatalf("n = %v", out["n"])
	}
}

func TestStructFieldsUseNativeForm(t *testing.T) {
	type carrier struct {
		When time.Time `json:"when"`
		Data []byte    `json:"data"`
	}
	c := carrier{When: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Data: []byte{1}}
	raw, err := serde.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"when":"2026-01-02T03:04:05Z","data":"AQ=="}`; string(raw) != want {
		t.Fatalf("Marshal = %s, want %s", raw, want)
	}
	var back carrier
	if err := serde.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !back.When.Equal(c.When) || string(back.Data) != "\x01" {
		t.Fatalf("roundtrip = %+v", back)
	}
}

func TestUnmarshalTypedDest(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	in := map[string]any{"when": when, "dur": 2 * time.Minute, "uid": testUUID, "raw": []byte{9}}
	raw, err := serde.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := serde.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if !m["when"].(time.Time).Equal(when) {
		t.Fatalf("when = %v", m["when"])
	}
	if m["dur"].(time.Duration) != 2*time.Minute {
		t.Fatalf("dur = %v", m["dur"])
	}
	if m["uid"].(uuid.UUID) != testUUID {
		t.Fatalf("uid = %v", m["uid"])
	}
	if string(m["raw"].([]byte)) != "\t" {
		t.Fatalf("raw = %v", m["raw"])
	}
}

func TestLargeIntegerExact(t *testing.T) {
	const big = int64(1234567890123456789)
	raw, err := serde.Marshal(map[string]any{"big": big})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := serde.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out["big"].(int64) != big {
		t.Fatalf("big = %v", out["big"])
	}
}

func TestRawMessagePassthrough(t *testing.T) {
	raw, err := serde.Marshal(map[string]any{"doc": json.RawMessage(`{"a": 1}`)})
	if err != nil {
		t.Fatal(err)
	}
	// json compacts raw-message output, so spacing is not preserved.
	if string(raw) != `{"doc":{"a":1}}` {
		t.Fatalf("Marshal = %s", raw)
	}
	var out struct {
		Doc json.RawMessage `json:"doc"`
	}
	if err := serde.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Doc) != `{"a":1}` {
		t.Fatalf("doc = %s", out.Doc)
	}
}

func TestUnsupportedTypes(t *testing.T) {
	if _, err := serde.Marshal(func() {}); err == nil {
		t.Fatal("func: want error")
	}
	if _, err := serde.Marshal(map[string]any{"c": make(chan int)}); err == nil {
		t.Fatal("chan: want error")
	}
}

func TestDefaultHook(t *testing.T) {
	v, err := serde.DefaultHook(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["__everystep_type__"] != "datetime" {
		t.Fatalf("hook = %v", v)
	}
	if _, err := serde.DefaultHook(make(chan int)); err == nil {
		t.Fatal("want error for chan")
	}
}

// boom is a host error that survives replay.
type boom struct {
	msg string
	n   int
}

func (b *boom) Error() string { return b.msg }
func (b *boom) Code() string  { return "test.boom" }
func (b *boom) Payload() any  { return map[string]any{"n": b.n} }

func TestEncodeErrorReplayable(t *testing.T) {
	enc := serde.EncodeError(&boom{msg: "kaboom", n: 3})
	if enc.Code != "test.boom" {
		t.Fatalf("code = %q", enc.Code)
	}
	if enc.Message != "kaboom" {
		t.Fatalf("message = %q", enc.Message)
	}
	var payload map[string]any
	if err := json.Unmarshal(enc.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["n"].(float64) != 3 {
		t.Fatalf("payload = %v", payload)
	}
}

func TestEncodeErrorWalksChain(t *testing.T) {
	err := fmt.Errorf("context: %w", &boom{msg: "inner", n: 1})
	enc := serde.EncodeError(err)
	if enc.Code != "test.boom" {
		t.Fatalf("code = %q", enc.Code)
	}
	if enc.Message != "context: inner" {
		t.Fatalf("message = %q", enc.Message)
	}
}

func TestEncodeErrorUnmatched(t *testing.T) {
	enc := serde.EncodeError(errors.New("plain failure"))
	if enc.Code != "" {
		t.Fatalf("code = %q", enc.Code)
	}
	if enc.Message != "plain failure" {
		t.Fatalf("message = %q", enc.Message)
	}
	if len(enc.Payload) != 0 {
		t.Fatalf("payload = %s", enc.Payload)
	}
}

func TestDecodeErrorRegistered(t *testing.T) {
	serde.RegisterError("test.boom", func(msg string, payload json.RawMessage) error {
		b := &boom{msg: msg}
		if len(payload) > 0 {
			var p struct {
				N int `json:"n"`
			}
			if err := json.Unmarshal(payload, &p); err == nil {
				b.n = p.N
			}
		}
		return b
	})
	enc := serde.EncodeError(&boom{msg: "kaboom", n: 3})
	var raw json.RawMessage
	raw, _ = json.Marshal(enc)
	err := serde.DecodeError(raw)
	var b *boom
	if !errors.As(err, &b) {
		t.Fatalf("err = %T %v", err, err)
	}
	if b.msg != "kaboom" || b.n != 3 {
		t.Fatalf("boom = %+v", b)
	}
}

func TestDecodeErrorUnregisteredCode(t *testing.T) {
	err := serde.DecodeError([]byte(`{"code":"no.such.code","message":"gone"}`))
	var sf *errs.StepFailure
	if !errors.As(err, &sf) {
		t.Fatalf("err = %T %v", err, err)
	}
	if sf.Msg != `gone (original error type "no.such.code" unavailable)` {
		t.Fatalf("msg = %q", sf.Msg)
	}
}

func TestDecodeErrorMessageOnly(t *testing.T) {
	err := serde.DecodeError([]byte(`{"message":"kaput"}`))
	var sf *errs.StepFailure
	if !errors.As(err, &sf) {
		t.Fatalf("err = %T %v", err, err)
	}
	if sf.Msg != "kaput" {
		t.Fatalf("msg = %q", sf.Msg)
	}
}

func TestDecodeErrorGarbage(t *testing.T) {
	err := serde.DecodeError([]byte(`not json`))
	var sf *errs.StepFailure
	if !errors.As(err, &sf) {
		t.Fatalf("err = %T %v", err, err)
	}
	if sf.Msg == "" || sf.Msg[0] == 'k' {
		t.Fatalf("msg = %q", sf.Msg)
	}
}

func TestTerminalRoundtrip(t *testing.T) {
	payload := map[string]any{"node_id": "a", "when": time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	enc := serde.EncodeError(&errs.Terminal{Reason: "node-full", Data: payload})
	var raw json.RawMessage
	raw, _ = json.Marshal(enc)
	err := serde.DecodeError(raw)
	var tt *errs.Terminal
	if !errors.As(err, &tt) {
		t.Fatalf("err = %T %v", err, err)
	}
	if tt.Reason != "node-full" {
		t.Fatalf("reason = %q", tt.Reason)
	}
	p := tt.Data.(map[string]any)
	if p["node_id"] != "a" || !p["when"].(time.Time).Equal(payload["when"].(time.Time)) {
		t.Fatalf("payload = %v", p)
	}
}
