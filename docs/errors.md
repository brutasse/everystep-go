# Errors

A step that returns an error records it and fails the run, unless the body
handles the error. Because the body replays, the error has to come back on
the next claim the same way it did the first time — same type, so
`errors.Is` and `errors.As` keep working, same message, so logs and reports
line up.

That is the problem this design solves: errors are values, stored in SQL,
and reconstructed on replay.

## The stored shape

Every recorded error is stored as three JSON columns:

```json
{ "code": "cloud.out_of_capacity", "message": "out of capacity in eu-west", "payload": { "region": "eu-west" } }
```

On replay the `code` selects a decoder and the payload is reconstructed
into a Go error. A code with no registered decoder degrades to a plain
failure (below). The `message` is always carried, even when the payload
cannot be decoded.

## Replayable

An error participates in replay by implementing:

```go
type Replayable interface {
	error
	Code() string      // stable identifier, e.g. "cloud.out_of_capacity"
	Payload() any      // JSON-serializable detail; nil for none
}
```

`EncodeError` walks the error chain (`errors.As`) and encodes the first
`Replayable` it finds. Wrappers are transparent: `fmt.Errorf("...: %w",
err)` round-trips to the inner typed error.

```go
type ErrOutOfCapacity struct{ Region string }

func (e *ErrOutOfCapacity) Error() string { return "out of capacity in " + e.Region }
func (e *ErrOutOfCapacity) Code() string  { return "cloud.out_of_capacity" }
func (e *ErrOutOfCapacity) Payload() any  { return map[string]any{"region": e.Region} }
```

A step that returns `&ErrOutOfCapacity{Region: "eu-west"}` records
`code=cloud.out_of_capacity` with the region in the payload. On replay the
registered decoder rebuilds the same value, and `errors.As` in the body
matches it.

## Registration

Built-in everystep errors are registered for you. A host error needs one
registration, at process start:

```go
func init() {
	es.RegisterError("cloud.out_of_capacity", func(msg string, payload json.RawMessage) error {
		var p struct {
			Region string `json:"region"`
		}
		if payload != nil {
			_ = json.Unmarshal(payload, &p)
		}
		return &ErrOutOfCapacity{Region: p.Region}
	})
}
```

The decoder receives the stored message and raw payload and returns the
reconstructed error. `es.RegisterError` is safe to call in `init`.

### Built-in codes

These five are registered by the library itself; the decoders are
idempotent and re-registering them is a no-op.

| code | error | notes |
| --- | --- | --- |
| `everystep.error` | `*es.EverystepError` | a step or call rejected before running (bad name, bad args, …). Never re-raised into the body on replay. |
| `everystep.terminal` | `*es.Terminal` | stops the run as `stopped`, not a failure. |
| `everystep.workflow_code_error` | `*es.WorkflowCodeError` | the body changed under an in-flight run (id/name mismatch). |
| `everystep.step_failure` | `*es.StepFailure` | the degraded form: a stored error whose decoder is missing. |
| `everystep.effect_uncertain` | `*es.EffectUncertain` | an unsafe step's effect may or may not have happened; the run is `blocked`. |

## The degraded path

If a stored error has no registered decoder — the code is unknown, the
library was upgraded, the decoder was removed — the run still replays. The
error comes back as `*es.StepFailure` carrying the original message and a
note that the original type is unavailable. The body's `if err != nil`
branch still runs; `errors.Is`/`errors.As` for the original type do not.

If the stored error was not encoded at all (no code), it comes back as
`*es.StepFailure` with the message. Either way the run does not wedge: a
missing decoder is a degraded replay, not a failure of the replay.

## Terminal is not a failure

`*es.Terminal` records as a failed step (the body replays the same
decision) but ends the run `stopped`. It is excluded from the failure
reporter. Use it when the precondition is gone and retrying is pointless —
see [the model](model.md).

## EffectUncertain is a hold, not a failure

`*es.EffectUncertain` means an unsafe step's effect may have happened. The
run goes `blocked` and waits for `es.ResolveStep`. It is never reported to
the failure reporter and never requeued on its own.

## Choosing between them

| situation | error | run ends |
| --- | --- | --- |
| the effect failed, retry later | any error (replayable or not) | `failed` |
| the precondition is gone | `*es.Terminal` | `stopped` |
| the effect may have happened | `*es.EffectUncertain` | `blocked`, until resolved |
| the step itself was invalid | `*es.EverystepError` | `failed` |
