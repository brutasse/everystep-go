package serde

import (
	"encoding/json"
	"errors"
	"fmt"

	errs "github.com/brutasse/everystep-go/internal/errors"
)

// EncodedError is the stored form of a recorded error. Errors that
// implement errs.Replayable carry a code and a payload; everything else
// stores the message only.
type EncodedError struct {
	Code    string          `json:"code,omitempty"`
	Message string          `json:"message"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// EncodeError encodes an error for storage, walking the chain for a
// Replayable.
func EncodeError(err error) EncodedError {
	enc := EncodedError{Message: err.Error()}
	var rp errs.Replayable
	if errors.As(err, &rp) {
		enc.Code = rp.Code()
		if p := rp.Payload(); p != nil {
			if raw, merr := Marshal(p); merr == nil {
				enc.Payload = raw
			}
		}
	}
	return enc
}

func init() {
	// Built-in decoders for the error types everystep itself raises and
	// records, so they rebuild on replay without the host registering them.
	RegisterError(errs.CodeEverystep, func(msg string, _ json.RawMessage) error {
		return &errs.EverystepError{Msg: msg}
	})
	RegisterError(errs.CodeTerminal, func(msg string, payload json.RawMessage) error {
		t := &errs.Terminal{Reason: msg}
		if len(payload) > 0 {
			_ = Unmarshal(payload, &t.Data)
		}
		return t
	})
	RegisterError(errs.CodeWorkflowCodeError, func(msg string, _ json.RawMessage) error {
		return &errs.WorkflowCodeError{Msg: msg}
	})
	RegisterError(errs.CodeStepFailure, func(msg string, _ json.RawMessage) error {
		return &errs.StepFailure{Msg: msg}
	})
	RegisterError(errs.CodeEffectUncertain, func(msg string, _ json.RawMessage) error {
		return &errs.EffectUncertain{Msg: msg}
	})
}

// DecodeError rebuilds a stored error. The decoder registered under the
// code is used when present; unknown, unregistered or unbuildable codes
// degrade to a *errs.StepFailure carrying the stored message.
func DecodeError(raw json.RawMessage) error {
	var enc EncodedError
	if err := json.Unmarshal(raw, &enc); err != nil {
		return &errs.StepFailure{Msg: fmt.Sprintf("step failed (unrecognized failure record: %s)", string(raw))}
	}
	if enc.Code != "" {
		if f, ok := errs.Lookup(enc.Code); ok {
			if err := f(enc.Message, enc.Payload); err != nil {
				return err
			}
		}
	}
	if enc.Message != "" {
		if enc.Code != "" {
			return &errs.StepFailure{Msg: fmt.Sprintf("%s (original error type %q unavailable)", enc.Message, enc.Code)}
		}
		return &errs.StepFailure{Msg: enc.Message}
	}
	return &errs.StepFailure{Msg: "step failed (original error unavailable)"}
}

// DecodeFunc rebuilds a stored error from its message and payload. It
// returns the reconstructed error, or nil if it cannot be built.
type DecodeFunc = errs.DecodeFunc

// RegisterError binds a decoder to an error code so errors with that code
// rebuild on replay.
func RegisterError(code string, decode DecodeFunc) {
	errs.Register(code, decode)
}
