# Effect Journal

Draft 6, for review. It updates draft 5; `spec.md` is the original.

What changed since draft 5:

- New names for the background operations (decision 17). `fork` is fire-and-forget: it returns `Unit`, records
  nothing in a replayable process, and is skipped at replay. `async` is new: it returns its `Fiber`, the process's
  next event waits for it, and its operations are recorded. `offload` does not change (sections 4, 15 and 16).
- Each effect log is in delivery order, and `EffectStore` checks that when it writes (decision 18). The index of runs
  and the question of an index in the footer are gone (sections 7, 8 and 9).
- Writing comes first. Stage 1 records `suspend` live; replay is stage 2 (section 22).
- Decision 13 is made: `Log.release()`, `Log.Config.sealOnStart`, `maxOpenLogs` with a default of 100, and a batch
  written in groups of at most `maxOpenLogs` logs. Forcing only a log's own directory and a lock on the store's map
  are dropped (section 8).
- Decision 14 is simpler: an omitted cause has no marker, so `FailureData` stays as it is (section 14).

How to read it:

- Text without a mark is what you set, what the code does today, or a detail that follows from the two.
- **Proposed** marks a choice I made where you left room. Each one is listed again in section 21.
- **Open** marks something that is not designed yet. It names its decision in section 21.

Decided:

- Recorded failures use structured failure data and `RecordedEffectException`, including converted causes, both
  live and during replay. Unrecorded execution keeps original exceptions (section 14).
- The outcome stores `FailureData`, not the exception. One function builds the exception from it, live and at replay
  (section 14).
- A stack frame is stored as a `StackTraceElement`, by the runtime's own codec, without Java serialization (section
  14).
- The stored record has no process reference. The recorder receives `(process, record)` pairs, and the sink routes by
  the process (sections 6 and 8).
- #85: `Codec` and `CodecAvailability` replace `EffectCodec` and `EffectCodecAvailability` (section 6).
- Decision 16: journals and stores stay separate classes, with no shared trait, and there is one store per journal
  (section 7).

- Decision 1: the path lives in `Scope`, the counters live in `ProcessState` (section 5).
- Decision 2: in a replayable process every effect is recorded, unless it is inside `liveOnly`. One that cannot be
  recorded raises `RecoveryContractViolation` (section 4).
- Decision 3: one recorder for all effect logs. Its sink forces the logs of a batch at the same time (section 8).
- Decision 4: batches stay. `Recorder` gets an optional flush interval (section 8).
- Decision 5: an outcome that cannot be stored fails the handler with `RecoveryContractViolation` (section 14).
- Decisions 7 and 12 were left to me. A sealed effect segment has its segment ID in its name, and the effect recorder
  closes first (sections 7 and 8). Decision 6, the index, is replaced by decision 18.
- Decision 8 is answered by decision 17 (section 16).
- Decision 13: at most `maxOpenLogs` effect logs hold an open file, 100 by default. A log drops its file with
  `release()`, and with `sealOnStart = false` its `init` continues a valid open file instead of sealing it. The store
  writes a batch in groups of at most `maxOpenLogs` logs (section 8).
- Decision 17: `fork` is fire-and-forget and records nothing; `async` waits, returns its `Fiber` and is recorded;
  `offload` stays (sections 15 and 16).
- Decision 18: the records of a process are in the order of their deliveries (section 9).
- Already in the code: `Recorder` takes the write mode in its configuration and has no `admitDurable`;
  `Recorder.Store` is `Recorder.Sink`; `DeliveryRecorder` is `DeliveryJournal`.

Written: the record types (`EffectEntry`, `EffectKey`, `PathSegment`, `EffectOutcome`, `FailureData`) and the codec
support they use (`SumCodec`, `ListCodec`, `OptionCodec`, the stack frame codec, `DecodeException`). Nothing else of
the effect journal is written.

## 1. Purpose

A `Replayable` process is rebuilt after a restart by running its handlers again on the recorded deliveries. That only
works if a handler does the same thing every time. So today the interpreter rejects every operation whose result can
differ between two runs: in a replayable process `suspend`, `race` and `fork` raise `RecoveryContractViolation`, and
only `eval` is left.

The effect journal records the outcome of such an operation the first time it runs. Replay returns the recorded
outcome and does not run the operation again.

```scala
suspend(callService()).flatMap(x => eval(use(x)))
```

- Live: `callService()` runs, its outcome is recorded, the handler continues with it.
- Replay: `callService()` does not run. The handler continues with the recorded outcome.

What is promised:

- An operation whose outcome is found at replay is not executed again.
- An outcome can be missing after a crash. `UnboundEffectPolicy` then decides (section 10). `Fail`, the default,
  stops recovery. `Execute` runs the operation again, so its external effect can happen twice.
- Exactly-once external effects are not promised in any mode.

## 2. What this design does not do

| not done | what happens instead |
|---|---|
| Restore running fibers, stacks, closures or captured variables | Only outcomes are replayed. A delivery is replayed by running its handler again from its start. |
| Restart background work | The process starts it again itself, from a lifecycle event (section 15). |
| Keep the delivery journal and the effect logs consistent with each other | They are written independently. After a crash a delivery can be there without some of its outcomes, and an outcome without its delivery (sections 10 to 12). |
| Force every outcome to storage | Outcomes follow `JournalConfig`, as deliveries do (section 8). |
| Hold back a snapshot while a fork is running | The snapshot is taken. Work the fork had not finished is not restored (section 13). |
| Deliver the sends a handler had not made before the crash | Replay skips sends, as today. |

The third row needs an example, because draft 3 prevented it and this draft does not.

P's handler for delivery 7 runs `suspend(getPrice)`, gets 100, and sends `Order(100)` to Q. Then the host crashes. Q's
delivery `Order(100)` survived. P's outcome did not.

- `Fail`: recovery stops at P's missing outcome.
- `Execute`: P runs `getPrice` again and may get 105. P's state is rebuilt from 105, Q's from 100.

That a surviving delivery never depends on a lost outcome is not a requirement of this design.

## 3. Terms

| term | meaning |
|---|---|
| delivery | One journaled event handed to a handler. It has a journal `seq`, unique across all processes. |
| operation | A recorded DSL operation: `suspend` first, later `race` and `async`. |
| outcome | How an operation ended: its value, or its failure. |
| branch | Operations that run one after another: a handler, an `async` body, one side of a race. |
| path | Where an operation sits inside its delivery (section 5). |
| effect log | The log that holds the outcomes of one process (section 7). |
| missing outcome | Replay reaches an operation and finds no record for it (section 10). |
| orphan | A recorded outcome whose delivery did not survive (section 12). |

## 4. Operations

| operation | live | replay | recorded |
|---|---|---|---|
| `eval` | runs | runs again | no |
| `register` | registers and starts | registers | by its registration record, as today |
| `halt` | runs | runs again | no |
| `send`, `forward`, `reply` | sends | skipped | no; the receiver's delivery is what the journal holds |
| `delay` | sleeps | returns at once | no |
| `offload` | starts background work; the next event waits for it | skipped | no |
| `suspend` | runs | returns the outcome | yes |
| `liveOnly` (new) | runs its body | skipped | no, and nothing inside it |
| `fork` | starts its body in the background; the next event does not wait for it | skipped | no, and nothing inside it |
| `async` (new) | starts its body in the background and returns its `Fiber`; the next event waits for it | runs its body again | its operations, under the delivery that started it |
| `race` | runs both sides | continues with the side that won live | the decision; the rest is **open** (section 17) |

The first six rows are today's behaviour and do not change. `fork` changes: it returns `Unit`, and in a replayable
process it behaves as if inside `liveOnly` (decision 17, section 15).

The rule for a replayable process with the journal enabled (decision 2, decided): **every `suspend`, `race` and
`async` is recorded, unless it is inside `liveOnly`. Nothing inside `fork` or `offload` is recorded.**

One that cannot be recorded raises `RecoveryContractViolation`:

| case | when it is raised |
|---|---|
| The call site has no codec. | Before the effect runs. |
| The operation is in the handler of a lifecycle event (`Initialize`, `Start`, `Restored` and the other system events). Those events are not journaled, so there is no delivery `seq` to record under. | Before the effect runs. The handler wraps such an effect in `liveOnly`. |
| The outcome cannot be stored (section 14). | After the effect ran. |
| The operation is `fork`, `async` or `race`, until their stages (section 22). | Before it runs, as today. |

A replayable process without an event codec raises it too. Today that case is an `IllegalStateException`.

## 5. Identity of an operation

```scala
final case class EffectKey(deliverySeq: Long, path: List[PathSegment])

final case class PathSegment(id: Int, kind: PathSegment.Kind)

object PathSegment:
  enum Kind:
    case Suspend, Async, Race, RaceLeft, RaceRight
```

Stage 1 writes only `Suspend`. The other kinds are declared already; new kinds are appended (section 6). `Async` is
today's `Fork`, renamed by decision 17. It keeps the ordinal, and nothing has been written with it.

- The key is the `seq` of the delivery and the path. The process is the log the record is in.
- Path segments are typed. The kind is not needed to make a path unique; it is there for the check in section 9 and
  for reading a log by eye.

### Numbering

- A branch has a prefix and a counter. The prefix of a handler is empty. The counter starts at 0.
- When a branch reaches an operation it takes the next number `k`.
  - `suspend`: the operation is `prefix/Suspend[k]`.
  - `async`: the body is a new branch with prefix `prefix/Async[k]`.
  - `race`: the race is `prefix/Race[k]`. Its sides are new branches with prefixes `prefix/RaceLeft[k]` and
    `prefix/RaceRight[k]`. This line belongs to the candidate in section 17.
  - `fork` takes no number: nothing inside it is recorded.
- **The parent takes the number before the body starts.** A body's prefix then depends on where the `async` stands
  in the parent's code, never on which fiber runs first.
- No two branches share a counter, so no number depends on scheduling.

```scala
async(suspend(a) ++ suspend(b)) ++ suspend(c)
```

| operation | path |
|---|---|
| the `async` | takes 1; the body's prefix is `Async[1]` |
| `suspend(a)` | `Async[1]/Suspend[1]` |
| `suspend(b)` | `Async[1]/Suspend[2]` |
| `suspend(c)` | `Suspend[2]` |

In `spec.md` the last one is `Suspend[1]`, which needs a counter per kind. Here a branch has one counter for all
kinds, so it is `Suspend[2]`. Both give unique paths; one counter is less state.

**An `async` body keeps the `seq` of the delivery that started it.** It ends before the process takes its next
event, so its records come before those of any later delivery (section 9).

`handleError`, `guarantee`, `withSender` and `suspend` of a flow add nothing to a path. They only shape the order in
which a branch reaches its operations.

### Where the path and the counters live (decision 1, decided)

- **The path lives in `Scope`.** A new key holds the branch the code is in, as an `EffectKey`: the `seq` of the
  delivery and the prefix. `Scope` stays immutable. An `async` gives its body a new scope with a longer prefix, and
  the body keeps that scope, and with it the `seq`, for as long as it runs.
- **The counters live in `ProcessState`:** a map from a branch to the last number it took.

```scala
// Scope
case object EffectBranch extends Key[EffectKey] // the branch the code is in: delivery seq and prefix

// ProcessState
val effectCounters: Map[EffectKey, Int] // branch -> the last number it took; safe for several fibers
```

The handler of delivery 7 runs with `EffectKey(7, Nil)` in its scope. Its first `suspend` takes number 1 from the entry
of `EffectKey(7, Nil)` and is `EffectKey(7, List(Suspend[1]))`. An `async` that takes number 2 runs its body with
`EffectKey(7, List(Async[2]))` in the body's scope.

What follows from this choice:

| rule | why |
|---|---|
| The map key is the branch's whole `EffectKey`, `seq` included. | It is what the scope holds. Branches of two deliveries of one process never count at the same time, because the next event waits for `async`. |
| The map is safe for several fibers. | A handler and its `async` bodies count at the same time. An entry is still used by one branch only, so no number depends on scheduling. |
| An entry is removed when its branch ends: the handler's when the handler ends, a body's when its fiber ends. | Otherwise the map grows by one entry per delivery. |
| At the start of every delivery the scheduler sets the key to `EffectKey(seq, Nil)`. It removes the key when the delivery is not a journaled delivery of a replayable process. | `Send` and `Forward` stamp the sender's scope on the envelope. Without this the receiver would start in the sender's branch. |
| After `MapScope` the interpreter puts the key back. | `MapScope` lets handler code replace the scope of its body. |

The `Interpreter` trait does not change, and no new type is needed.

The alternative that was not taken: an object per branch that the interpreter passes down, with the prefix and a
plain counter in it. It needs no map and no cleanup, but it is a new type and a fourth `interpret` in the public
`Interpreter` trait.

### The whole path is stored

A segment is 8 bytes: the id and the kind, 4 bytes each (section 6). A path has one segment for a plain `suspend`, and
one more for each `async` or race around it. An id with a parent id is not enough, because ids repeat under different
ancestors: `Async[1]/Suspend[1]` and `Async[2]/Async[1]/Suspend[1]` are both "id 1, parent id 1".

### No envelope ID

The key has no envelope ID. That is safe only because a delivery `seq` is unique across all processes and is never
used twice, across restarts and across deleted files. Section 12 is what makes that true. The envelope ID could not
replace it: a deterministic restart can pair the same `seq` with the same envelope ID again.

## 6. The record

```scala
final case class EffectEntry(key: EffectKey, outcome: EffectOutcome)

sealed trait EffectOutcome

object EffectOutcome:
  final case class Success(result: Array[Byte]) extends EffectOutcome
  final case class Failure(data: FailureData)   extends EffectOutcome
  final case class Cancelled()                  extends EffectOutcome
```

- Not in the record: the process reference (it is in the segment header), the envelope ID, a delivery-start marker.
  The recorder receives the process with each record so that the sink can route it (section 8); the log stores the
  record without it.
- **`Success`** holds the value encoded with the call site's `Codec[A]`. The call site passes it through
  `CodecAvailability[A]` (#85), so a process that is not replayable needs no codec. A replayable process without one
  raises `RecoveryContractViolation` before the effect runs. The codec owns its version: whatever it needs to read
  older values is inside its own bytes.
- **`Failure`** is structured failure data, encoded by the runtime, including converted causes (section 14).
- **`Cancelled`** was meant for a cancelled `join`. `join` needs no record now (section 16), so nothing writes it yet;
  the case stays for cancellation (decision 10). A replayed `suspend` that finds it treats it as a mismatch.
- **The record has no schema.** Draft 4 compared a stored schema with the call site's at replay. Without one, a value
  type that changed at the same path is noticed only when the old bytes fail to decode, and not at all when they
  happen to decode as the new type (section 9).

### Encoding (Proposed, decision 15)

Every record type derives its codec: `EffectEntry`, `EffectKey`, `PathSegment`, `PathSegment.Kind`, `EffectOutcome`
and `FailureData`. Only the stack frame codec is written by hand. All numbers are big-endian.

| type | bytes |
|---|---|
| `EffectEntry` | the key, then the outcome |
| `EffectKey` | `deliverySeq` (8), the number of path segments (4), then each segment |
| `PathSegment` | `id` (4), then `kind` as its ordinal (4) |
| `EffectOutcome` | the ordinal of its case (4): `Success` 0, `Failure` 1, `Cancelled` 2. Then the fields of the case. |
| `Success` | the length of the value (4), then the value's bytes |
| `FailureData` | the type name, the message as a tag and a string, the frames as a count and the frames, the cause as a tag and failure data |
| a string | its length in UTF-8 bytes (4), then the bytes |
| an `Option` | tag 0 for none; tag 1, then the value |

A derived format follows the order of fields and the order of cases. So:

- **Cases are only appended.** Inserting, removing or reordering a case of `PathSegment.Kind` or `EffectOutcome`
  changes the meaning of records already stored. A race decision is appended as a new case of `EffectOutcome`.
- **Fields are not changed in place.** A different record format is a new `LogCodec` version, and the log keeps a
  codec that reads the old version, as `DeliveryLog.lookupCodec` does.
- **A test pins the bytes** of one record of each case. A change to a derived format then fails that test, not the
  reading of old segments.

Not taken: hand-written codecs for every type, or explicit ids per case as `JournalEntry.Kind` has. They write the
same kind of bytes, and keeping the format still depends on a person not changing it; the pinned bytes check that in
either case.

## 7. Storage: one log per process

```
<dataDir>/high-water                          shared with the delivery journal (section 12)
<dataDir>/effects/<hex of process reference>/
    open                                      the segment being written
    running                                   marker while the log is open
    <min seq>-<max seq>-<segment id>.jrl      sealed segments
```

Each directory is one `Log`, built like `DeliveryLog`:

| part | content |
|---|---|
| header | the process reference |
| entry | one record (section 6) |
| footer | lowest and highest delivery `seq` in the segment |
| state | the same two numbers |
| codec version | 1 |

Frames, checksums, sealing, the trailer and recovery of the open file are the ones `Log` already has.

- The delivery journal and the effect logs are separate logs.
- A process's log is created when its first outcome is written. A process that records nothing has no directory.
- At most `maxOpenLogs` effect logs hold an open file at a time (section 8, decision 13).
- **Records are in delivery order** (section 9, decision 18), so two segments share at most the `seq` at their border.
  One long handler can still fill several segments with records of one delivery, so `<min>-<max>` can repeat
  (`5-5-3.jrl`, `5-5-4.jrl`), and the name needs the segment ID. `Log.fileName` therefore also receives the segment ID
  (decision 7).
- Configuration is `JournalConfig`: `dataDir`, `batchSize`, `writeMode`, `durability`, `maxSegmentBytes`,
  `maxEntryBytes`. Three fields are added, `unboundEffectPolicy`, `flushInterval` and `maxOpenLogs` (section 8). There
  is no separate configuration for effects.

### Classes (decision 16, decided)

```
DeliveryJournal ─ Recorder ─ DeliverySink ─ JournalStore (JournalStoreLocal) ─ DeliveryLog          (one log)
EffectJournal   ─ Recorder ─ EffectSink   ─ EffectStore                      ─ EffectLog per process (many)
                                                     both stores share one HighWaterStore
```

- **Journals and stores stay separate classes.** There is no shared journal trait and no shared store trait. The two
  journals differ in their key, the order of their logs, the number of logs, and how they are read, and no caller
  uses both through one type. The parts that are shared are already generic: `Recorder`, `Recorder.Sink`, `Log` and
  `HighWaterStore`.
- **`EffectLog`** is the `Log` of one process, as `DeliveryLog` is the log of all deliveries.
- **`EffectStore` is one store for all effect logs.** It is a concrete class. `JournalStore` is a trait because
  `ParApp.journalStorage` lets an application plug in its own delivery store; nothing asks for that for effects. The
  store does the work that needs every log in view:
  - routing: it splits a batch by process, checks the order, and syncs the logs it wrote to at the same time
    (section 8);
  - the bound on open logs (section 8);
  - boot: it opens every effect directory, those of processes that are not registered included, and gives the
    highest `seq` (section 12);
  - replay, in stage 2: it reads the records of one delivery (section 9);
  - truncation, later: it stores the high water before it deletes segments (section 12).
- **`EffectJournal` is a `Recorder` in front of the store**, like `DeliveryJournal`. It orders the calls that involve
  both: `truncate` flushes before it truncates, and `close` closes the recorder before the store. Its sink,
  `EffectSink`, is a private class inside it, as `DeliverySink` is inside `DeliveryJournal`.
- The store can be tested without a recorder, as `JournalStoreLocal` is.

Not taken: a store per log. `EffectLog` already is the object of one log, and the work listed above spans all logs,
so a store per log would still need one owner above the stores. That owner would be the real store.

**Proposed:** the methods of stage 1, which writes only.

```scala
final class EffectJournal[F[_]]:
  def admit(process: ProcessRef.Unknown, entry: EffectEntry): F[Unit] // waits under WriteAhead
  def runWriter: F[Unit]                                              // supervised by ParApp
  def flush(): F[Unit]
  def maxSeq: F[Option[Long]]                                         // the highest seq in any effect log
  def close(): F[Unit]                                                // the recorder, then the store
```

- The store has the same methods without the recorder, plus `open()` and `append` for the sink.
- The recorder's ids are not used. A record is found by its delivery `seq` and path, not by an id.
- Reading comes in stage 2 (section 9, decision 19), and truncation once the runtime calls it (section 13).

## 8. Live: writing an outcome

`suspend`, live:

1. Read the branch from the scope and take its next number (section 5). That gives the path.
2. Run the effect. It can be cancelled, and a cancelled effect records nothing.
3. Encode the outcome on the handler's fiber: a value with the call site's codec, a failure as structured failure data.
   If it cannot be encoded, or the record is larger than `maxEntryBytes`, the handler fails (section 14).
4. Admit the record to the recorder with `admit`. Under WriteAhead it waits until the record is durable.
5. Continue with the original successful value, or raise `RecordedEffectException` constructed from the failure
   data, including converted causes. Replay constructs the same failure representation. No disk read-back is needed.

Once the effect has ended, steps 3 and 4 cannot be cancelled: an effect that finished gets its record admitted.

### The recorder

Handlers and `async` bodies never write to a log. They admit records to a `Recorder`, and its writer is the only thing
that appends to an effect log. The recorder holds `(ProcessRef.Unknown, EffectEntry)` pairs: the process routes the
record, and the record is what is stored.

One `Recorder` serves all effect logs (decision 3, decided). Its sink takes a batch and hands it to `EffectStore`,
which does four things:

1. It splits the batch by process, and keeps the order of admission inside each process.
2. It checks the order (decision 18): a record whose `seq` is below the last one written to its process's log fails
   the batch before any of it is written. No program can produce one, so a failure is a bug, and the application
   stops as for any sink failure. The store keeps each log's highest `seq`, read at boot or when the log is created.
3. It appends each record to the log of its process.
4. It syncs the logs it wrote to, all at the same time, and completes when the last sync returns.

Steps 3 and 4 run once per group of at most `maxOpenLogs` logs (section 8, "Open logs").

`Log.sync` forces under HostCrash and does nothing under ProcessCrash. The writer is supervised next to the delivery
recorder's writer in `ParApp.run`, and a failed write stops the application.

Step 4 syncs the logs at the same time, and not one after another, because forces on different files overlap. The
time until one new 128-byte record in every log is durable, measured on the development machine with
`channel.force(true)`:

| logs | one writer, one force after another | one writer, forces at the same time | a writer per log |
|---|---|---|---|
| 1 | 3.5 ms | 3.4 ms | 3.4 ms |
| 10 | 38–78 ms | 7–13 ms | 10 ms |
| 100 | 460–480 ms | 20–22 ms | 26 ms |
| 400 | 1.6–1.7 s | 42–46 ms | 31–35 ms |

It is one disk, two passes and few rounds, not a proper benchmark.

The last column is the alternative that was not taken, a recorder per process. It is as fast, and each log has its
own batches. But it parks one writer thread per process, and each writer has to be started, closed and supervised
with its process.

### Open logs (decision 13, decided)

An open effect log holds one file handle, on its `open` segment. With a log per process, 10,000 processes that record
would hold 10,000 handles. So the number of effect logs that hold their file is bounded:

- `JournalConfig.maxOpenLogs` is the bound, 100 by default. It applies to effect logs; the delivery journal has one
  log. The limit on open files belongs to the whole OS process and also counts sockets, the JAR files the JVM keeps
  open and the application's own files. The journal cannot count those, so it takes a fixed share, which the
  application can change.
- The store keeps every effect log it has opened. At most `maxOpenLogs` of them hold their file, kept in a
  `LinkedHashMap` in access order, least recently used first. No new type.
- The map needs no lock in stage 1: boot fills it before the writer starts, and after that only the writer uses it.
  Whether reading in stage 2 uses it is decided with the reader (decision 19).

The writer, for one batch:

```
split the batch by process, keeping the order of admission inside each process
check the order (section 8, "The recorder")
for each group of at most maxOpenLogs logs:
  release logs outside the group until the group fits    they were synced with an earlier batch or group
  append each log's records                              a released log opens its file again
  sync the group's logs at the same time                 nothing under ProcessCrash
complete the batch
```

- A batch that touches at most `maxOpenLogs` logs is one group, which is the write of section 8.
- Under HostCrash a released log was synced before, so releasing forces nothing, and a batch across 300 logs with the
  default bound is three rounds of forces, each run at the same time.
- The order inside each process does not change: all of a process's records in a batch are in one group. Under
  WriteAhead the handler still waits for the whole batch.

Three changes to `Log`. None of them is specific to effects.

| change | what it does | why |
|---|---|---|
| `Log.Config.sealOnStart: Boolean = true` | `true` is today's behaviour. With `false`, `init` continues the open file when it would discard no bytes, after a clean stop or a crash; the report still says whether the stop was clean. A damaged file is sealed as today, and so is one written with another codec version than the current one, because a segment uses one version. | Effect logs set it to `false`; otherwise every close and reopen leaves one small sealed segment. `DeliveryLog` keeps `true`. |
| `Log.release()` | Closes the open file's channel. The `Log` keeps its state, its counters and the `running` marker. Under HostCrash it first forces the file, but only when something was appended since the last sync. The next `append` opens the file again at its end. `close()` after `release()` still records the clean stop. | Reopening through `close` and `init` reads the whole open segment to rebuild the state, reads the header of every sealed segment, and under HostCrash forces every directory up to the root: about ten forces. A released log reopens with one `FileChannel.open`. |
| `Log.fileName` receives the segment ID | Decision 7. | Segment names can repeat their `seq` range (section 7). |

A released log is still open as far as recovery is concerned: its `running` marker stays, so a crash while it is
released is an unclean stop.

What follows:

- In stage 1 boot opens every effect log once with `init`, reads its highest `seq` from `readFooters` and
  `readActive`, and releases it (section 9). The open file of a cleanly stopped log keeps its records.
- A log that is created during the run is opened with `init` on its first write.
- Not taken: an option to force only a log's own directory. Under HostCrash every `init` forces every directory up to
  the root, which is the price of that durability. HostCrash is not the mode for high traffic: with WriteAhead and
  one fiber it measured 343 operations per second.

### When a record is stored

| write mode | the handler continues | the record reaches its log |
|---|---|---|
| Buffered | right after admission | when its batch is written |
| WriteAhead | after its batch is written and synced | before the handler continues |

| durability | a written batch survives |
|---|---|
| ProcessCrash | a process crash |
| HostCrash | a host crash; the logs the batch wrote to are forced, all at the same time |

No mode forces once per outcome. PR 78 measured a force at about 3 ms.

### When a Buffered batch is written (decision 4, decided)

`Recorder` keeps its batches. It writes a batch when it is full (`batchSize`, 1024 by default), when it is flushed, or
when it is closed. Nothing in the runtime calls `flush`, so a batch that does not fill stays in memory until shutdown.

For outcomes that means this: an application handled 5,000 deliveries and recorded 500 outcomes, then it crashes.
Four delivery batches were full and are on disk. The 500 outcomes were in memory and are gone. Replay finds 4,096
deliveries and no outcome, and with `Fail` the next start stops at the first `suspend`.

`Recorder` therefore gets an optional flush interval:

```scala
// Recorder.Config
flushInterval: Option[FiniteDuration] // None: a batch is written only when it is full, flushed or closed

// JournalConfig
flushInterval: Option[FiniteDuration] = None
```

- With an interval, the writer waits for work no longer than the interval. When the wait ends and the active batch
  holds records, the writer writes that batch.
- It is the writer's own wait that ends, so there is no timer thread and no new type.
- Under load nothing changes: batches fill and are written when full. The interval only matters when the writer has
  been idle for that long.
- A crash then loses about one interval of outcomes, plus the batch that was being written.
- It is one setting for both journals, because there is no separate configuration for effects. `None` keeps today's
  behaviour, and the example above then still happens.
- `batchSize` stays the one setting it is today. A smaller batch is written sooner but costs more forces under load:
  with 100 records per batch and 3.4 ms per force, the delivery journal could write at most about 29,000 records per
  second.

### Other rules

- A record that encodes above `maxEntryBytes` is not admitted, and the handler fails (section 14). The size is
  checked on the handler's fiber, because a failed write in the recorder is terminal.
- A sink failure is terminal, as for deliveries: the writer fails and the application stops.
- At shutdown the effect recorder is closed before the delivery recorder (decision 12). A crash between the two then
  leaves orphans, which are harmless, and not missing outcomes.

## 9. Replay

Writing comes first: stage 1 writes, and this section is built in stage 2 (section 22). Only the order of the log and
the boot of stage 1 are part of stage 1.

### The log is in delivery order (decision 18, decided)

The records of a process are in the order of their deliveries:

- Recorded operations run in the handler and in `async` bodies, and the process takes its next event only after both
  have ended (section 16). `fork` and `offload` record nothing.
- Under `Execute`, replay appends a missing outcome at the end of the log. That is where it belongs: a log loses only
  its tail, and a process handles one delivery at a time, so no record of a later delivery survives after a lost
  record.
- A new delivery gets a `seq` above every record on disk, orphans included (section 12).

Inside one delivery the records of its branches, the handler and its `async` bodies, interleave.

The store checks the order when it writes (section 8). The check guards against bugs: no program can produce a record
below the last one of its log.

Not taken: draft 5's `fork`, whose child could outlive its delivery. Records of different deliveries then interleaved,
and replay needed an index of runs per delivery, `(segmentId, offset, length)`, built by a scan, or kept in each
footer. In the footer it would be one item per delivery: about 13 MB for a 32 MB segment of small records, and more in
the memory of every open log.

### Boot

Stage 1:

1. Every directory under `effects/` is opened as a log, with `sealOnStart = false`: a valid open file is continued, a
   damaged one is sealed. Its highest `seq` is read from `readFooters` and `readActive`, and the log is released
   (section 8).
2. The `seq` counter continues after the highest `seq` found anywhere (section 12).

Stage 2 adds: processes are registered and restored, and deliveries are replayed in `seq` order, as today.

**Open, stage 2:** whether boot reads the files directly instead of opening each log, once the reader exists.

### Reading the records of a delivery (stage 2)

Replay asks for the deliveries of a process in `seq` order, and the records of one delivery sit together. So a
process's records are read forward. Reading starts at the first segment whose highest `seq`, in its name, is at or
above the first delivery replayed, and goes on from where it stopped. Nothing is indexed.

**Open (decision 19): the reader.**

- **Its shape.** `read(key): Option[EffectOutcome]`, your proposal, or the records of one delivery in log order. A
  keyed read loses a check: a `suspend` that replaced an `async` at the same place finds no record for its key, and
  under `Execute` it runs again, where a read of the delivery's records stops recovery. The check that no record is
  left over needs the delivery's records too. **Proposed:** an `EffectReader` per process whose read returns the
  records of one delivery, in log order. The matching rules below stay in replay.
- **Files.** **Proposed:** it keeps no file open between reads. It needs no `release()` then, and replay needs no bound
  on open files.
- It finds a segment by its ID each time it opens one, because the writer may seal the open file in the meantime
  (under `Execute`). Sealing a damaged open file cuts its damaged tail and appends the footer, so the positions of its
  valid records do not change.

`suspend` at path `p` in delivery N, at replay:

1. Take the next number. That gives `p`.
2. Take the delivery's next record.
3. **There is one, its path is `p`, and its outcome is `Success`:** decode the value with the call site's codec and
   continue with it. The effect does not run.
4. **There is one, its path is `p`, and its outcome is `Failure`:** raise the `RecordedEffectException` built from its
   failure data (section 14). The effect does not run.
5. **There is one and its path is not `p`, or its outcome is `Cancelled`:** the handler is not the code that wrote the
   log. Recovery stops. The path holds the kind, so a `suspend` that replaced an `async` at the same place is caught
   here.
6. **There is none:** the outcome is missing (section 10).

When the delivery ends, every record of it must have been taken. A record left over means an operation was removed
from the handler, and recovery stops.

With `async` a delivery has several branches whose records interleave, so "the next record" becomes the next record
of the branch, found by its path among the delivery's records (stage 3). The format does not change for it.

### What stops recovery

| at replay | meaning | result |
|---|---|---|
| The next record has another path than the call site, or an outcome the operation cannot have | The handler is not the code that wrote the log. | Recovery stops before any effect runs. |
| The handler ends and records are left | Same. | Recovery stops. |
| A value or a failure cannot be decoded | The application codec cannot decode the value, or the runtime cannot decode the failure format. | Recovery stops. |
| A segment is damaged | Corruption, as in the delivery journal. | Recovery stops. |
| The outcome is missing | Section 10. | The policy decides. |
| The handler fails with a recorded failure, as it did live | An application failure. | Recovery continues. |

A value type that changed at the same path is caught only by the decoding row, and only when the old bytes fail to
decode (section 6).

The last row changes `Recovery.runHandler`. Today any handler failure during replay fails boot, while live it is
reported to the sender and the process goes on. With recorded failures a handler fails at replay exactly as it did
live, so only the errors of this table may stop recovery. They are raised as `RecoveryContractViolation`, which
already passes through `handleError` untouched.

### Memory

- **No index.** Reading forward needs none.
- **Records:** with the proposed reader, the records of one delivery at a time.
- **There is no memory budget.** "One delivery at a time" is not one: a single delivery can record without limit
  (section 18). A segment's highest `seq` says when a segment is no longer needed; it does not limit memory.

## 10. A missing outcome

Replay reaches an operation and its delivery has no record for it. Replay cannot tell which of these happened:

- The operation never started.
- It was running when the crash came. Its effect may have happened.
- It finished and its record was not stored. In Buffered mode the handler may have continued and acted on it.

`JournalConfig.unboundEffectPolicy` decides:

- **`Fail`** (default): recovery stops with an error that names the process, the delivery `seq` and the path. No file
  is changed.
- **`Execute`**: the operation runs now and its outcome is recorded. Its effect can happen a second time, and its
  result can differ from the one the live handler saw. The application has to make such operations safe to repeat.

After a missing operation has run under `Execute`, the handler goes on as a replayed handler: its `eval`s run and its
sends are skipped.

With `Fail`, a crash that leaves any replayed delivery without one of its outcomes stops the next start. In Buffered
mode that is the usual result of a crash (section 8). The application then starts only with `Execute`.

## 11. Crash and recovery cases

### A. WriteAhead

Delivery N is stored before its handler starts, and each outcome is stored before its handler continues. "Stored"
means what the durability setting says: under ProcessCrash it survives a process crash only.

| # | crash point | after restart | replay |
|---|---|---|---|
| 1 | Operation `k` has not started, is running, or has finished and its record is not stored | N and outcomes `1..k-1` | `1..k-1` are returned. `k` is missing. |
| 2 | Record `k` is stored; the handler has not finished | N and outcomes `1..k` | `1..k` are returned. Then the next operation is missing, or the handler ends. |
| 3 | The handler finished | N and all its outcomes | Every outcome is returned. |

At most one outcome is missing per branch that was running.

### B. Buffered

What survives is what was written before a process crash, or forced before a host crash. Each log keeps the front
part of what was admitted to it and loses the rest: the delivery journal of all deliveries, each effect log of its
process's outcomes. How much each one keeps does not depend on the others.

| # | delivery N | its outcomes | replay |
|---|---|---|---|
| 1 | survived | all survived | Every outcome is returned. |
| 2 | survived | lost from operation `k` on, although the live handler may have gone past them | `k` is missing. Under `Execute` it runs again, and after it each later operation of the branch in turn. |
| 3 | lost | some survived | They are orphans (section 12). N is not replayed and the message is lost, as today. Effects its handler ran stay in the outside world. |
| 4 | lost | lost | Nothing is replayed. |

Under ProcessCrash durability a host crash is not covered in either mode.

### C. Other cases

| case | result |
|---|---|
| Crash while a batch is being written | The open file ends in a cut frame. `Log` removes it when the log is next opened. The records after the cut are lost: table B, row 2. |
| Crash while the logs of one batch are written or forced | Some logs have their records of the batch, others do not. Each log still keeps a front part. |
| Crash while an effect log seals or starts a segment | `Log` recovery, as for the delivery journal. |
| Crash during recovery | Recovery starts again. An outcome it recorded under `Execute` is returned if it was written, and the operation runs once more if not. |
| Clean shutdown while an effect is running | A handler that the shutdown cancels records nothing for that effect. Its delivery is in the journal, so at the next start the outcome is missing (decision 10). |
| A record cannot be written (disk full) | The recorder fails and the application stops. |
| An outcome cannot be stored (section 14) | The handler fails with `RecoveryContractViolation` and the application stops. Nothing is recorded for the operation, so at the next start its outcome is missing. |
| A snapshot of P at `seq` S exists | Deliveries of P up to S are not replayed, so their outcomes are never read. |
| The handler code changed between crash and restart | The checks of section 9. An operation that was added, removed or reordered is found when a path does not fit, or when a value does not decode. One that was replaced by another of the same kind is not found when the old bytes decode as its value. |
| The original exception class is missing or changed | Failure reconstruction still works; the original type name is stored as text and its class is not loaded. |
| The process is no longer registered | Its effect log stays. Only its highest `seq` is read at boot. |

## 12. Orphans and the high water

An orphan is a recorded outcome whose delivery did not survive (table B, row 3).

- **Replay never returns an orphan.** Its delivery is not in the journal, so the delivery is not replayed, and the
  forward read of section 9 passes over its records.
- **An orphan still counts for the `seq` counter.** If the counter restarted below it, a new delivery of the same
  process could get the orphan's `seq`, and a later replay would return the orphan's outcome for that new delivery. A
  `seq` below the orphan's would also break the order of the log, and the store's check would stop the application
  (section 8). So stage 1, which only writes, already needs this rule.

### Where the counter is restored from

At boot the `seq` counter continues after the highest `seq` in all of these:

- the delivery journal;
- the `high-water` file;
- the restored snapshots;
- **every effect log directory, including those of processes that are not registered.** A process can be registered
  later in the run under a reference that already has a log.

These maxima only prevent a `seq` from being used twice. They say nothing about where it is safe to snapshot or
truncate.

### Before evidence is deleted

A sealed segment's footer is the proof of the `seq`s it held. Draft 3 said effect truncation does not need the
high-water file. That was wrong: an effect log can hold the highest `seq` there is, for example an orphan.

- **Before an effect log deletes segments, the highest `seq` of those segments is stored in the `high-water` file.**
  The file is forced before the first segment is deleted. `DeliveryLog.truncate` already does this.
- The stored value only grows. `HighWaterStore.advance` reads the file, merges the maxima and writes only a greater
  value, inside one lock.
- **The delivery log and all effect logs must share one `HighWaterStore` instance.** Two instances for one directory
  each read, merge and write on their own, and the later write can put back a lower value. Today `JournalStoreLocal`
  creates its own instance, so that has to change.
- An effect record has no envelope ID. It merges into the file with envelope ID 0, which changes nothing there.
- Separate JVMs on one directory are not coordinated, as for the delivery log.

## 13. Snapshots and truncation

- A snapshot is not held back by a `fork` that is still running. A `fork` records nothing, so the snapshot covers no
  half-recorded work.
- Restoring a snapshot does not start again the fibers that deliveries covered by it had forked.
- So the work such a `fork` had not finished is lost at restore. If it has to finish, the process starts it again
  from a lifecycle event (section 15). There is no durable fiber and no snapshot barrier for children.
- An `async` body ends before its process takes its next event, so a snapshot taken between deliveries never covers
  half of one.

### Truncating an effect log

After a snapshot of process P at `seq` S is stored:

1. Take the sealed segments from the oldest forward, up to the first whose footer ends above S.
2. Store their highest `seq` in the `high-water` file.
3. Delete them.

- It is the prefix-only `Log.truncate`. A segment that holds records both below and above S stays whole.
- Each process is truncated by its own snapshot.
- Nothing in the runtime calls truncation yet, for effect logs or for the delivery journal. Wiring it is not part of
  this work.

## 14. Failures and cancellation

### Failures

**Decided:** recorded effect failures are exposed as `RecordedEffectException` both live and during replay.
The conversion boundary is the recorded `suspend`.

| execution | exception seen by DSL error handling outside `suspend` |
|---|---|
| Replayable process with journaling enabled, live execution | `RecordedEffectException` |
| Replay of a recorded failure | `RecordedEffectException` |
| Journaling disabled, an ordinary process, or inside `liveOnly` | Original exception |

Enabling recording therefore changes exception-class matching outside `suspend`. This is part of the public
contract. Error handling inside the supplied effect still sees original exceptions before they cross this boundary.
Cancellation, recovery contract violations and journal infrastructure errors retain their separate handling.

The runtime extracts structured data:

```scala
final case class FailureData(
    exceptionType: String,
    message: Option[String],
    stackTrace: List[StackTraceElement],
    cause: Option[FailureData]
) derives Codec
```

- A frame is a `StackTraceElement`: the type `Throwable.getStackTrace` returns and `setStackTrace` takes, so the
  frames go back onto the exception unchanged. The runtime's own codec stores its fields: class loader, module and
  module version, class, method, file and line. There is no Java serialization.
- No original `Throwable` is serialized. Nonserializable custom fields, including fields on a cause, are ignored.
- Causes are converted recursively. Each cause becomes another `RecordedEffectException`, both live and during
  replay. If the original exception has no cause, the converted exception has no cause.
- The original type name is exposed as data. The converted exception is not an instance of that original class;
  `case _: IOException` does not match a converted `IOException`.
- Live execution constructs the exception from the same failure data that is encoded, without reading from disk.
  It does not attach the original throwable as its cause, which would expose different cause classes during replay.
- Extraction stops at a cause it has already seen, which ends a cycle, and after 32 causes (decision 14).
- A missing or changed original exception class does not prevent replay. Compatibility is required for the runtime
  failure-data format, not for Java serialization of application exception classes.

**Proposed (decision 14):** an omitted cause has no marker.

- The chain is cut after 32 causes, counted from the outer exception, or at a cause already seen. The last converted
  exception then has no cause, as when there is none.
- Live and replay both raise the exception built from the same `FailureData` (below), so both see the same cut
  chain. A marker would only say that something was cut; the two agree without it.
- So `FailureData` stays as it is, and there is no new type. Draft 5 proposed a three-case enum for the cause, and a
  `Boolean` beside the `Option` was the other option.
- Suppressed exceptions (`getSuppressed`) are not recorded in stage 1. Recording them later is a new record format
  (section 6).

### `RecordedEffectException`

```scala
// io.parapet.exceptions
final class RecordedEffectException private[parapet] (
    val exceptionType: String,
    message: Option[String],
    cause: Option[RecordedEffectException]
) extends RuntimeException(message.orNull, cause.orNull)
```

- Only the runtime builds one, so the constructor is `private[parapet]`. One function, `toException(FailureData)`,
  builds it, live and at replay, so both raise the same exception by construction.
- Applications see `exceptionType`, and the usual `getMessage`, `getStackTrace` and `getCause`. `FailureData` stays
  internal.
- `toString` prints the original type, for example `RecordedEffectException(java.io.IOException: disk full)`. The
  default would print only the message, and logs would lose the type.
- The recorded frames are set with `setStackTrace`. `fillInStackTrace` is overridden to return `this`, so the stack of
  the code that builds the exception is not captured. `writableStackTrace = false` is not used, because it makes
  `setStackTrace` do nothing.

The outcome stores `FailureData`, not the exception:

```
live:    Throwable --extract--> FailureData --encode--> log
                                FailureData --toException--> raised
replay:  log --decode--> FailureData --toException--> raised
```

- One function builds the exception for both paths. If the outcome held the exception, live would build it from the
  `Throwable` and replay from the bytes, and the two would have to agree.
- A `Throwable` is mutable and reaches user code, which can call `addSuppressed` or `setStackTrace` on it. A stored
  record should not be such an object.
- `FailureData` derives its codec. A codec for the exception would be written by hand, against its getters and its
  private constructor.

### An outcome that cannot be stored (decision 5, decided)

Three cases: failure-data extraction or encoding fails, a value's codec fails to encode it, or a record is larger than
`maxEntryBytes`.

**The handler fails with `RecoveryContractViolation`.** It names the process, the delivery `seq`, the path and the
reason, and it carries the original error as its cause.

- It is raised when the outcome is encoded, which is after the effect ran. The effect is not undone.
- It is a violation like the others: `handleError` does not catch it, the worker raises it again, and the application
  stops. The problem cannot be missed.
- Nothing is recorded for the operation. At the next start its delivery is replayed and the outcome is missing
  (section 10). Under `Execute` the effect runs again, and fails the same way until the code is fixed.
- No replacement for a recording failure is stored. `RecordedEffectException` represents a successfully recorded
  application failure; it does not hide a failure to record an outcome.

Not taken: a codec for failures at every call site, and a diagnostic record that lets the live handler continue. The
second hides the problem until the next recovery.

### Cancellation

Cancellation is not a failure and is never stored as one.

- A cancelled `suspend` records nothing. That covers shutdown, `halt`, the losing side of a race, and `fiber.cancel`.
- `join` on an `async` fiber needs no record: replay runs the body again and gets the same outcome (section 16).
- `cancel` on an `async` fiber raises `RecoveryContractViolation` in a replayable process until this is designed: a
  cancelled body stops at a point the log does not mark, and at replay it would run past that point.
- The exact rules are **open** (decision 10). The case that matters first: a clean shutdown that cancels a handler in
  the middle of an effect leaves a missing outcome, and with `Fail` the next start stops.

## 15. `liveOnly`, `fork` and `offload`

```scala
def liveOnly[A](flow: => Free[C, A]): Free[C, Unit]
def fork[A](flow: Free[C, A]): Free[C, Unit] // returns Fiber[F, A] today
```

`liveOnly` is new. `fork` changes (decision 17).

| | `liveOnly` | `fork` | `offload` (today) | `async` (section 16) |
|---|---|---|---|---|
| runs | inline; the handler waits for it | in the background | in the background, on the blocking pool | in the background |
| returns | `Unit` | `Unit` | `Unit` | its `Fiber` |
| the process's next event | after the handler | does not wait | waits for it | waits for it |
| at replay | skipped | skipped | skipped | runs again |
| recorded | nothing inside | nothing inside | nothing inside | its operations |

- In a replayable process `fork` behaves as if inside `liveOnly`. It returns `Unit` because a skipped `fork` has no
  fiber to return.
- Every `fork` in the code today is fire-and-forget and ends in `.void`: the timeout in `Channel` and the receive
  loops of `ServerProcess`, `DatagramProcess` and `DuplexProcess`. They keep their name, and nothing uses `join` or
  `cancel` on a forked fiber.
- Returning `Unit` is not what makes them unrecorded: a `suspend` that returns `Unit` is recorded, because its record
  is what stops the effect from running again.
- Inside the body, `suspend`, `race` and `async` run as in a process that is not replayable. Nothing is recorded.
- `liveOnly` says what happens at replay. `unrecord` would only say what happens live.
- `fork` does not wait because it records nothing: it cannot put a record out of order. The risk it keeps is a state
  change that replay needs, as with `offload` today, and no runtime check can see that.

Rules for the application:

- **Skipped code must not make a state change that replay needs.** Replay rebuilds state without it.
- A background result that has to change replayable state is sent to the process as an event. That delivery is
  journaled and replayed.
- A loop that has to keep running is started from a lifecycle event, not from the handler of an old delivery. That
  handler's `fork` is skipped at replay, so after a restart nothing would start the loop.

**Open (decision 11):**

- **Who owns a `fork` fiber**, for stage 3. Today's `fork` starts a fiber that nobody owns: it is not cancelled when
  its process stops, and its failure is seen only by `join`. Once `fork` returns `Unit` there is no `join`, so a
  failure inside it is lost, and the fiber outlives its process. Your earlier list had such work owned by the
  process, cancelled when the process stops, with its failures reported. That needs `fork` to register its fiber
  with the process, as `offload` does.
- **Where `liveOnly` is skipped.** **Proposed:** exactly where `send` and `offload` are skipped today, which is while
  `context.replaying` is true. During boot that is true for replayed deliveries and also for `Initialize` and
  `Restored`, which recovery runs before it goes live. `Start` is sent after boot is live, so today `Start` is the
  only lifecycle event that always runs live. If background work should also start from `Initialize` or `Restored`
  at boot, recovery has to run those handlers live.

## 16. `async`

```scala
def async[A](flow: Free[C, A]): Free[C, Fiber[F, A]]
```

Decided (decision 17):

- `async` starts its body in the background, as `fork` does today, and returns its `Fiber`. The handler goes on; it
  waits for the body only if it joins.
- **The process takes its next event only after the body has ended**, as with `offload`. `async` registers its fiber
  with the tracker `offload` uses (`Context.offloads`), so the scheduler needs no new wait.
- Its operations are recorded under the `seq` of its delivery, with the prefix `.../Async[k]` (section 5). The body
  ends before the next delivery starts, so the log stays in delivery order (decision 18).
- At replay the body runs again: its `eval`s run, and its `suspend`s return their recorded outcomes.
- `join` needs no record. Replay runs the body again and gets the same outcome as live.
- A body that had not finished when the crash came has no record for its next operation: a missing outcome
  (section 10).
- `cancel` raises `RecoveryContractViolation` in a replayable process until cancellation is designed (decision 10). A
  cancelled body stops at a point the log does not mark; at replay it would run past that point and find an outcome
  that was never recorded.
- A snapshot never covers half of a body (section 13).

These answer the five questions of decision 8: replay runs the body again, the next delivery waits for it, an
unfinished body has a missing outcome, `join` needs no record, and `cancel` is a violation for now.

**A known gap.** Inside one delivery, the handler and its `async` bodies run at the same time, and nothing records
how their `eval`s interleave. The paths stay deterministic, because each branch numbers its own operations. If two
branches change the same state, replay may interleave them differently; that is already a race live. The
application keeps such a change in one branch, or sends it as an event.

Not taken: draft 5's recorded `fork`, whose child could outlive its delivery. Records of different deliveries then
interleaved, which needed overlapping segments, an index of runs and late records after a snapshot, and left
decision 8 open.

Until stage 3, `fork` and `async` in a replayable process raise the violation, as `fork` does today.

## 17. Race

Fixed: **replay continues with the side that won live.**

```scala
race(suspend(callService()), delay(5.seconds))
```

Working the winner out again at replay gets it wrong. `delay` returns at once at replay, so the timeout would win
even when the call won live. And when both sides finished close together, both have outcomes and nothing says which
one the handler saw.

Candidate, not agreed: the race records which side won at `Race[k]`, and replay runs only that side.

**Open (decision 9):**

1. The losing side is cancelled live at a point the log does not mark. State it changed before that is not repeated
   if replay does not run it.
2. Whether the losing side's `suspend`s are recorded at all, since replay would never read them.
3. A race with no decision record: is the whole race missing, even when one side has all its outcomes?
4. Both sides fail, or both are cancelled.

Until then `race` in a recorded delivery stays a violation.

## 18. Loops

```scala
def poll: DslF[F, Unit] = suspend(fetch()).flatMap(x => eval(apply(x))) ++ poll
```

- Every turn appends a record under the same delivery. Segments roll, which limits the size of a file, not the size
  of the history.
- None of it can be deleted. Snapshots are taken between deliveries, and this delivery never ends, so no snapshot
  ever covers it.
- Replay of that delivery reads all of it.

What works:

- **One step per delivery.** The handler does one step and sends itself an event for the next. Each step is its own
  delivery, a snapshot can be taken between two steps, and truncation follows the snapshot.
- **A loop that is not replayed.** `fork` or `liveOnly`, with results sent back as events (section 15). It is not a
  drop-in replacement for a recorded loop that changes replayable state directly.

## 19. Costs and limits

- **Files.** One directory per process that has an effect log, and at most `maxOpenLogs` open files. In stage 1 boot
  opens every effect log once, with `init`.
- **Groups.** A batch that touches more logs than `maxOpenLogs` is written in groups (section 8). Each group is synced
  with its forces at the same time, and a log is released only after its sync, so releasing forces nothing. A batch
  across 300 logs with the default bound is three rounds of forces instead of one.
- **Forces under HostCrash.** Each effect log is its own file, so a batch needs one force per log it wrote to. They
  run at the same time (section 8): a batch across 100 logs took about 21 ms on the development machine, and across
  400 about 44 ms. In WriteAhead mode a handler waits for the slowest force of its batch.
- **One batch for all processes.** The writer writes one batch at a time, and a batch holds `batchSize` records, 1024
  by default. With the times above that is at most about 49,000 records per second across 100 logs, and about 23,000
  across 400. A larger `batchSize` raises it, but the delivery journal uses the same setting. In Buffered mode,
  records admitted faster than that wait in memory; the recorder's queue has no limit.
- **What a crash loses in Buffered mode.** The outcomes that are still in memory. Without `flushInterval` that is up
  to one batch, which for a quiet application is everything since it started. With it, about one interval.
- **Threads.** One writer thread for all effect logs. While a batch is forced, up to one more thread per log in it.
- **Boot.** Every effect log is opened once: its open file is read to continue it and again for its highest `seq`,
  and under HostCrash `init` forces every directory up to the root (section 8).
- **Memory at replay.** Section 9. No budget.
- **Changed handler code.** Paths belong to the code that wrote them. Replay does not survive a change that adds,
  removes or reorders operations, and one such change is not detected (section 11, table C). Take snapshots before
  deploying such a change, so that nothing old is replayed.
- **A lost delivery leaves its effects behind.** Table B, row 3.
- **Stored failures are structured data.** Original exception classes are not needed during replay. Custom exception
  fields and original class-based matching are not preserved. The runtime must retain compatibility with its failure format.

## 20. Code

Types. Per your rule none of them is written before you agree.

| type | state | what it is |
|---|---|---|
| `EffectKey`, `PathSegment`, `PathSegment.Kind` | written | Section 5: no process reference, no envelope ID. `Kind.Fork` is renamed `Kind.Async` (decision 17). |
| `EffectEntry`, `EffectOutcome` | written | One record and its outcome (section 6). |
| `FailureData` | written | Structured failure data (section 14). |
| `SumCodec`, `ListCodec`, `OptionCodec`, the stack frame codec, `DecodeException` | written | Codec support in `io.parapet`. |
| `Codec`, `CodecAvailability` | exist (#85) | The call site's codec for a value. They replace `EffectCodec` and `EffectCodecAvailability`. |
| `EffectLog` | agreed | The `Log` of one process (section 7). |
| `EffectStore` | agreed | All effect logs: routing, the order check, the open-log bound, boot; later reading and truncation (section 7). |
| `EffectJournal` | agreed | A recorder in front of the store (section 7). |
| `EffectSink` | agreed | The recorder's sink, a private class inside `EffectJournal`. |
| `RecordedEffectException` | agreed | Section 14. Public, in `io.parapet.exceptions`. |
| `LiveOnly` | agreed, stage 1 | New DSL operation (section 15). |
| `UnboundEffectPolicy` | agreed, stage 2 | `Fail`, `Execute`. `JournalConfig.unboundEffectPolicy`, default `Fail`. |
| `EffectReader` | **Open**, stage 2 | Your proposal: reads the records of one process (section 9, decision 19). |
| `Async` | agreed, stage 3 | New DSL operation (section 16). |

"Agreed" means the type is in this spec and you have not objected to it. None of them is written yet.

Changes to existing code:

| where | change | stage |
|---|---|---|
| `Log` | `Log.Config.sealOnStart`. `release()` closes the file handle and keeps the state; `append` opens it again; `close()` after it records the clean stop (decision 13). | 1 |
| `Log.fileName` | Also receives the segment ID (decision 7). `DeliveryLog` follows. | 1 |
| `Recorder` | An optional flush interval (section 8). | 1 |
| `Dsl` | `suspend` and `Suspend` carry a `CodecAvailability[A]`; `liveOnly` is added. | 1 |
| `RecoveryContractViolation` | Carries a reason and an optional cause in place of the operation name. It is raised for every case of sections 4, 9 and 14. | 1 |
| `Scope` | A key for the branch the code is in (section 5). | 1 |
| `ProcessState` | The counters of its branches (section 5). | 1 |
| `DslInterpreter` | Reads the branch from the scope and puts the key back after `MapScope` (section 5). `Suspend` follows section 8, and section 9 in stage 2. `LiveOnly` follows section 15. | 1 |
| `Scheduler` | Sets the branch key in the scope at the consume point, and removes the handler's counter when the handler ends (section 5). A missing event codec raises `RecoveryContractViolation`. Closes the effect journal before the delivery journal. | 1 |
| `Recovery` | The `seq` counter continues after the highest `seq` of the effect logs (section 12). | 1 |
| `JournalConfig` | `maxOpenLogs` and `flushInterval`; `unboundEffectPolicy` in stage 2. | 1 |
| `Context`, `ParApp` | Create the effect journal and supervise its writer next to the delivery recorder's. The effect journal receives the `Parallel[F]` that `ParApp` has, to sync the logs of a group at the same time. | 1 |
| `Recovery` | The boot steps of section 9. A handler failure no longer fails boot, except the errors of section 9. A missing event codec raises `RecoveryContractViolation`. | 2 |
| `Dsl`, `DslInterpreter` | `fork` returns `Unit` and behaves as if inside `liveOnly` in a replayable process; `async` is added (sections 15 and 16). | 3 |
| `JournalStoreLocal` | Receives the `HighWaterStore` instead of creating it, so that it shares the instance with `EffectStore` (section 12). | with truncation |

## 21. Decisions

Decisions 1 to 8, 12, 13 and 16 to 18 are made; 6 is replaced by 18, and 8 is answered by 17. Stage 1 still needs
decisions 14 and 15 and the second half of 11; each step names what it needs (section 22). Decisions 9, 10, 19 and
the first half of 11 belong to later stages.

| # | question | options | proposed |
|---|---|---|---|
| 1 | Where a branch's path and counter live (section 5) | The path in `Scope` and the counters in `ProcessState`. Or an object per branch that the interpreter passes down, and a fourth `interpret`. | **Decided:** the path in `Scope`, the counters in `ProcessState` |
| 2 | What a replayable process records (section 4) | | **Decided:** every effect, unless it is inside `liveOnly`. One that cannot be recorded raises `RecoveryContractViolation`. |
| 3 | How many recorders (section 8) | One for all effect logs, or one per process as in your README. | **Decided:** one for all. Its sink forces the logs of a batch at the same time. |
| 4 | When a Buffered batch is written if it does not fill (section 8) | As today: when full, flushed or closed. Or as soon as the writer is free. Or after an interval. | **Decided:** batches stay, and `Recorder` gets an optional flush interval |
| 5 | An outcome that cannot be stored (section 14) | a: recording failure. b: failure codecs. c: diagnostic record. | **Decided:** a, raised as `RecoveryContractViolation` |
| 6 | The index and its memory (section 9) | Build by a scan at boot, or store each segment's runs when it is sealed and load those. Whether there is a memory budget. | **Replaced** by decision 18: the log is in delivery order and is read forward, so there is no index. No memory budget. |
| 7 | Unique names for effect segments (section 7) | `Log.fileName(segmentId, state)`. Or keep `Log` as it is and add the `seq` and path of the segment's first record to the name. | **Decided** (left to me): `Log.fileName(segmentId, state)` |
| 8 | Replay of `fork`, `join` and `cancel` (section 16) | | **Answered** by decision 17: replay runs an `async` body again, the next delivery waits for it, `join` needs no record, and `cancel` is a violation until decision 10. `fork` records nothing. |
| 9 | Recording and replay of `race` (section 17) | Not designed beyond "the live decision is kept". | |
| 10 | Cancellation rules (section 14) | Not designed beyond "recorded where the program observes it". | |
| 11 | `liveOnly` and `fork` details (section 15) | First half, stage 3: who owns a `fork` fiber, nobody as today or the process. Second half, stage 1: where `liveOnly` is skipped, and whether `Initialize` and `Restored` run live-only code at boot. | **Proposed:** the process owns it; `liveOnly` is skipped while `context.replaying` |
| 12 | Closing order at shutdown (section 8) | The effect recorder first, or the delivery recorder first. | **Decided** (left to me): the effect recorder first |
| 13 | How many effect logs hold an open file, and what releasing one costs (section 8) | No bound. A bound, releasing with `close` and `init`. A bound, releasing with `release()`. | **Decided:** a bound, `maxOpenLogs`, 100 by default; `release()`; `Log.Config.sealOnStart`; a batch written in groups, each synced before its logs are released. Not taken: forcing only a log's own directory, and a lock on the store's map. |
| 14 | The omitted cause, the depth limit and suppressed exceptions (section 14) | A `Boolean` beside the `Option`, a three-case enum, or no marker. Depth limit. Suppressed exceptions now or later. | **Proposed:** no marker; the chain is cut after 32 causes or at a cause already seen; suppressed exceptions not recorded in stage 1 |
| 15 | How the record is encoded (section 6) | Derived codecs, or hand-written ones, or explicit ids per case. | **Proposed:** derived, with cases only appended and a test that pins the bytes |
| 16 | Classes of the effect journal (section 7) | A shared journal and store trait. A store per log. One `EffectStore` for all logs behind `EffectJournal`. | **Decided:** separate classes, no shared trait, one `EffectStore` for all effect logs |
| 17 | Names of the background operations (sections 15 and 16) | `async` fire-and-forget and a recorded `fork` that waits. Or `fork` fire-and-forget and a recorded `async` that waits. | **Decided:** `fork` is fire-and-forget, returns `Unit` and records nothing; `async` returns its `Fiber`, the next event waits for it, and it is recorded; `offload` stays |
| 18 | The order of an effect log (section 9) | Records of different deliveries interleave, read through an index of runs. Or the log is in delivery order. | **Decided:** delivery order, checked by `EffectStore` when it writes |
| 19 | The reader of stage 2 (section 9) | `read(key): Option[EffectOutcome]`, or the records of one delivery. A file kept open between reads, or none. | **Proposed:** the records of one delivery, in log order; no file kept open |

## 22. Stages

1. **`suspend`, recorded live.** Writing only. Each step ends with its tests, and the build stays green after each.

   | step | what | needs |
   |---|---|---|
   | 1a | `Log`: `sealOnStart`, `release()`, `fileName` with the segment ID. `DeliveryLog` follows. | decision 13 |
   | 1b | `Recorder`: the optional flush interval, and `JournalConfig.flushInterval`. | decision 4 |
   | 1c | `liveOnly`: the DSL operation and its interpreter case, skipped while `context.replaying`. | decision 11, second half |
   | 1d | The record: `derives Codec` on the record types, `Kind.Async`, the test that pins the bytes. | decision 15 |
   | 1e | Failures: extraction with the cycle check and the depth limit, `RecordedEffectException`, `toException`. | decision 14 |
   | 1f | Storage: `EffectLog`, `EffectStore` with boot, the order check and the groups, `EffectJournal` with `EffectSink`. Tested with `readSegment`, `readFooters` and `readActive`, without the runtime. | 1a, 1d |
   | 1g | Runtime: `JournalConfig.maxOpenLogs`, `RecoveryContractViolation` with a reason, the branch key in `Scope` and the counters in `ProcessState`, `suspend` with `CodecAvailability`, the live `suspend` of section 8, the scheduler, `Context` and `ParApp`, and the boot `seq` rule. | 1b, 1e, 1f |

   - Steps 1a to 1e are small and do not depend on each other, so they can be done in any order. 1f and 1g are the
     write path.
   - Until 1g a replayable process raises `RecoveryContractViolation` at `suspend` live and at replay, as today. From
     1g on a live `suspend` is recorded, and replay still raises the violation until stage 2. So an application could
     run `suspend` live and then fail to restart: stages 1 and 2 are released together.
2. **`suspend`, replayed.** The reader (decision 19), the boot and replay rules of section 9, `unboundEffectPolicy`,
   and the change to `Recovery.runHandler`.
3. **`fork` and `async`** (decision 17), after the first half of decision 11. `fork` returns `Unit` and records
   nothing; `async` is added, with `join`, and `cancel` raises the violation.
4. **`race`**, after decision 9.
5. **`cancel`** on an `async` fiber, after decision 10.

## 23. What other systems do

Each row is from the project's own documentation, as summarised by a web search. I have not read every page in full.

| system | what it does | what this design takes |
|---|---|---|
| [Restate](https://docs.restate.dev/services/versioning) | Replay must issue the same operations in the same order. A difference is a journal mismatch error and the invocation fails. | Position plus a check of the path, which holds the kind (section 9). |
| [DBOS](https://docs.dbos.dev/faq) | A step's output is stored under the workflow and a step counter. | The key: the delivery and a number taken in order (section 5). |
| [Golem](https://learn.golem.cloud/develop/durability) | An interrupted call is retried by default. With that turned off the agent fails instead. | The two policies of section 10. Golem's default is the opposite of ours. |
| [Temporal](https://docs.temporal.io/encyclopedia/retry-policies) | Activities are retried without limit by default. | The same trade as `Execute` and `Fail`. |
| [Temporal failure converter](https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/failure/DefaultFailureConverter.java) | Converts exceptions and causes to failure records, then reconstructs framework exceptions for the workflow, including during initial execution. | Normalize failures across the recorded `suspend` boundary both live and during replay (section 14). This reference is implementation/documentation, not a paper. |
| [Spark](https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/ExceptionFailure.html) | A task failure carries diagnostic fields and an optional serialized exception wrapper. | Not adopted: Parapet stores structured failure data without serializing the original exception. |
| [Bitcask](https://docs.riak.com/riak/kv/2.0.4/setup/planning/backend/bitcask/) | An index in memory maps each key to a file, a position and a size. At startup it is rebuilt by scanning the data files, or faster from hint files that hold the positions without the values. | Draft 5 took its index; with the log in delivery order (decision 18) a forward read needs none. |
| [TiKV Raft Engine](https://www.infoq.com/articles/raft-engine-tikv-database) | Many Raft groups share one log; an index in memory maps log keys to positions and is rebuilt from the log at restart. It dropped hint files because of their background writes. | Nothing now that there is no index. Its single shared log is not taken: you chose a log per process. |

## 24. Debt

- `Recovery.replay` reads the whole delivery journal from 0 into memory. It can read segment by segment from the
  lowest snapshot `seq`.
- Truncation is not called by the runtime (section 13).
- In Buffered mode a batch that does not fill is written only at close, unless `flushInterval` is set (section 8).
- Durability is set for the whole application. A setting per process would not isolate processes: one writer serves
  all effect logs, so under WriteAhead a HostCrash process's forces delay every process in its batch.
- In stage 1 boot opens every effect log with `init` (section 9).
