# The execution model

A workflow is an ordinary Go function. The engine records the outcome of
every step it calls in SQL. When a run is claimed again — after a crash, a
restart, a handoff to another worker — the body re-runs from the top and
every step whose outcome is already recorded is served from the database.
The first unrecorded step is the one that actually runs.

Everything in this document is a consequence of that.

## Steps and workflows

```go
type CreateVMArgs struct {
    Name string
    Size string
}

var createVM = es.Step("CreateVM", func(ctx context.Context, a CreateVMArgs) (string, error) {
    return cloud.CreateVM(ctx, a.Name, a.Size) // the effect, exactly your code
}, es.UnsafeToRepeat())

var tagVM = es.Step("TagVM", func(ctx context.Context, vmID string) (string, error) {
    return cloud.TagVM(ctx, vmID, "ready")
})
```

- A step has a name, a typed args value, and a typed result. `struct{}`
  works for zero args.
- The name is the step's identity. Go has no stable function address, so
  identity is a declared contract: renaming a step is a breaking change for
  runs that are in flight, exactly like changing the body.
- `s.Call(ctx, args)` outside a run executes the function directly and
  returns the result. No recording.

A workflow wraps a body function:

```go
func ProvisionVMBody(ctx context.Context, a CreateVMArgs) (string, error) {
    vmID, err := createVM.Call(ctx, a)
    if err != nil {
        return "", err
    }
    return tagVM.Call(ctx, vmID)
}

var provisionVM = es.Workflow("provision_vm", ProvisionVMBody)
```

Steps are durable only when called from a workflow body (or from inside a
`es.Parallel` fork). Calling a step directly in application code runs it
plainly.

## Scheduling

```go
runID, err := provisionVM.Schedule(db, CreateVMArgs{Name: "web-1", Size: "m"})
```

The args are serialized immediately, a run row is inserted as `scheduled`,
and the run id is returned. Workers pick it up from the queue.

To make scheduling transactional, pass a `*gorm.DB` obtained from
`Begin()`: the run row joins the transaction — claimable on commit, gone on
rollback.

```go
tx := db.WithContext(ctx).Begin()
defer tx.Rollback()

if err := hostWrites(ctx, tx); err != nil { // your own inserts
    return err
}
runID, err := provisionVM.Schedule(tx, args, es.Idempotency(orderID))
if err != nil {
    return err
}
return tx.Commit().Error
```

`es.Idempotency(key)` adds a unique constraint on (workflow name, key). A
duplicate schedule returns the existing run instead of creating a second
one; the first args win. This requires the gorm instance to be opened with
`Config.TranslateError: true`, which the idempotency match relies on.

## Step identity: the dotpath

Every step call in a run gets an id — a dotpath. Unnamed steps take the next
position in their scope; named steps take their name; a fork opens a scope
per branch and consumes a position itself.

- `1`, `2` — top-level steps, in body order.
- `2.0.1` — step 1 of branch 0 of fork 2.
- `attach-ip` — a named step uses its name as the segment.

Naming is a call option, checked before the step runs (a `*es.EverystepError`
is returned, nothing recorded):

- non-empty, no dots, no whitespace, not purely numeric,
- unique within its scope (a body, or one branch),
- the full dotpath is at most 300 characters.

A named step consumes a position like an unnamed one: adding or removing a
name never shifts the ids of the steps around it, which is what keeps
identity stable when the body is edited.

On replay the engine re-derives the same dotpath from the body. If an id
now maps to a different step name, the run fails with
`*es.WorkflowCodeError` — the body changed under an in-flight run, and
reusing the recorded result would be wrong.

## Recording

Every step call writes one row: the dotpath, the step name, the serialized
args, and the outcome.

| status | meaning |
| --- | --- |
| `done` | the function returned; the result is stored |
| `failed` | the function returned an error; the error is stored |
| `started` | the effect was claimed, no outcome was recorded |

A plain (safe-to-repeat) step records `done` or `failed` after the effect.
On replay, a recorded `done` serves the result without calling the function;
a recorded `failed` re-raises the stored error — the body's `if err != nil`
branch replays, including any durable cleanup it contains.

## At-least-once by default

A plain step records its outcome after the effect runs. A crash between the
effect and the record leaves no row; the next claim re-runs the effect.
Plain steps are for effects that are safe to repeat or for pure reads.

## UnsafeToRepeat

`es.UnsafeToRepeat()` marks a step whose effect must not run twice —
attaching an IP, sending a message, moving money.

The attempt is claimed in the database before the effect: the `started` row
is inserted in the step's own transaction, and the unique
(workflow run, dotpath) constraint makes the claim atomic. Two racing
claimants cannot both insert; the loser gets a duplicate-key error and
returns `*es.EffectUncertain` instead of running the effect a second time.

Replay of a `started` row means the previous attempt was never recorded —
the process died between the claim and the record. The engine does not know
whether the effect happened. The run goes to `blocked` and the step returns
`*es.EffectUncertain`. The run holds there until it is resolved, because
only you can say whether the effect happened.

A `started` row can also mean the crash landed before the effect — between
the claim insert and the call. Same state, same resolution: you check
externally what actually happened.

## Blocked runs and resolution

`blocked` is a holding state, not a failure: it is never requeued on its
own and never reported to the failure reporter.

Resolution is explicit, one of exactly three answers:

```go
// The effect happened; this is its result. The step records `done`
// and the body resumes past it.
es.ResolveStep(ctx, db, runID, stepID, es.WithResult(attachedIP))

// The effect happened and failed. The step records a failure (stored as
// *es.StepFailure) and the body's durable cleanup replays.
es.ResolveStep(ctx, db, runID, stepID, es.WithError("attach timed out; IP released manually"))

// The effect did not happen. The started row is deleted and the step
// re-runs.
es.ResolveStep(ctx, db, runID, stepID, es.Discard())
```

Resolution is allowed only on a `started` step of a blocked run. It clears
the run's error, releases the claim, and returns the run to `scheduled` —
any worker can claim it. Pass exactly one option.

## Terminal

`*es.Terminal` stops a run for a known reason without being a failure — the
precondition is gone and there is nothing to retry:

```go
var provisionVM = es.Workflow("provision_vm", func(ctx context.Context, a CreateVMArgs) (string, error) {
    running, err := isStillRunning(ctx, a.Name) // a plain read, or a step
    if err != nil {
        return "", err
    }
    if !running {
        return "", &es.Terminal{Reason: "VM already deleted upstream"}
    }
    // ...
    return "", nil
})
```

The terminal is recorded as a failed step (so the body replays the same
decision on the next claim) and the run ends `stopped`. `errors.As`
distinguishes it from a failure: it is never reported to the failure
reporter and it never blocks.

## Reading previous results

Inside a body, read a step that earlier calls may have recorded:

```go
if out, ok := es.Outcome(ctx, "create-vm"); ok && out.Status == "done" {
    // out.Result is the recorded result, json.RawMessage
}

var vmID string
if err := es.Result(ctx, "create-vm", &vmID); err == nil {
    // vmID decoded from the recorded result
}
```

The shared store is read-only: it holds recorded values, it never mutates
them, and it does not write anything. Name the step you are reading; the
result comes back as it was recorded, serialized to JSON.

A named step is the natural partner for this: it is the same id on every
replay, which is what makes "was this already done" a stable question.

## What must be serializable

Args are serialized up front — for the whole run before the run row is
inserted, for a step before its effect — and results are serialized before
they are recorded. If a value does not marshal to JSON, it fails there,
not mid-run.

The wire format is plain JSON with four tagged types, used in dynamic
positions (map values, `any` fields, slices of `any`):

| Go type | on the wire |
| --- | --- |
| `time.Time` | RFC3339Nano string, tagged `datetime` |
| `time.Duration` | total seconds, tagged `timedelta` |
| `uuid.UUID` | string, tagged `uuid` |
| `[]byte` | base64 string, tagged `bytes` |

Struct fields use Go's native JSON encoding. The same codec is applied when
reading recorded values, so a round trip is exact for these types.
