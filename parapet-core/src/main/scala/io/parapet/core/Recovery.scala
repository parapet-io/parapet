package io.parapet.core

import com.typesafe.scalalogging.Logger
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.journal.JournalEntry
import io.parapet.core.snapshot.{Snapshot, Snapshotable}
import io.parapet.effect.Effect
import io.parapet.effect.Monad.*
import io.parapet.{Envelope, Event, ProcessRef, Scope}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/** Owns application boot: registers the initial processes, restores each from its latest snapshot, then re-folds the
  * recorded journal onto them so every process catches up from its snapshot boundary to the last recorded delivery.
  *
  * The re-fold runs with the runtime in [[BootMode.Replaying]], so the interpreter suppresses sends and effects. Unlike
  * live delivery it drives the handler directly - it does not record the delivery, stamp a new `seq`, or fire snapshot
  * triggers, so it never re-enters the recording seam. Restore uses the [[Context]] registration hook (shared with
  * dynamic re-spawn); [[Recovery]] owns the ordering.
  */
final class Recovery[F[_]](context: Context[F], interpreter: Interpreter[F])(using effect: Effect[F]):

  private val logger = Logger(LoggerFactory.getLogger(classOf[Recovery[?]]))

  /** Advances the delivery-sequence and envelope-id counters past what the journal already holds, so a restart
    * continues rather than reissuing positions. Call before any envelope is created (i.e. before binding the
    * scheduler).
    */
  def seedSequencers(): F[Unit] =
    for
      maxSeq <- context.journalMaxSeq
      maxId  <- context.journalMaxEnvelopeId
      _      <- effect.delay {
        maxSeq.foreach(context.continueSeqAfter)
        maxId.foreach(Envelope.continueIdAfter)
      }
    yield ()

  private val recovered = scala.collection.mutable.Set.empty[ProcessRef.Unknown]

  /** Boots `processes`: register and recover each - restore + [[io.parapet.core.Events.Restored]], recursing into the
    * children each `Restored` handler re-spawns - then re-fold the recorded journal onto the whole tree. Runs under
    * [[BootMode.Replaying]] throughout and returns to [[BootMode.Live]] before the caller starts the scheduler.
    */
  def boot(processes: List[Process[F, ?, ?]]): F[Unit] =
    effect.delay { context.bootMode = BootMode.Replaying } >>
      processes.foldLeft(effect.pure(()))((acc, p) => acc >> context.register(ProcessRef.SystemRef, p).void) >>
      processes.foldLeft(effect.pure(()))((acc, p) => acc >> recover(p)) >>
      reFold() >>
      effect.delay { context.bootMode = BootMode.Live }

  /** Recovers one process and, when it was restored, the subtree it re-spawns.
    *
    * Restores state from the latest snapshot; a restored process is delivered [[io.parapet.core.Events.Restored]]
    * synchronously so its handler re-spawns children *before* the re-fold, then each child registered under it is
    * recovered in turn. A process with no snapshot is queued an [[io.parapet.core.Events.Start]] instead, delivered
    * live once the scheduler starts so its initial work is not suppressed. `recovered` guards against re-processing.
    */
  private def recover(process: Process[F, ?, ?]): F[Unit] =
    if recovered.contains(process.ref) then effect.pure(())
    else
      recovered.add(process.ref)
      restoreState(process).flatMap {
        case true  => deliverLifecycle(process.ref, Events.Restored) >> recoverChildren(process.ref)
        case false => context.sendStartEvent(process.ref).void
      }

  /** Restores `process` from its latest snapshot and records its restored `seq`, without any lifecycle event. Returns
    * `true` if a snapshot was found and applied, `false` if the process started fresh.
    */
  private def restoreState(process: Process[F, ?, ?]): F[Boolean] =
    val ref: ProcessRef.Unknown = process.ref
    process match
      case snapshotable: Snapshotable =>
        context.snapshots.fold(effect.pure(Option.empty[Snapshot]))(_.restoreLatest(ref, snapshotable)).flatMap {
          case Some(restored) =>
            effect
              .delay {
                context.continueSeqAfter(restored.metadata.seq)
                context.getProcessState(ref).foreach(_.restoredSeq = restored.metadata.seq)
              }
              .map(_ => true)
          case None => effect.pure(false)
        }
      case _ => effect.pure(false)

  /** Restores `process`'s state once, if it hasn't already been recovered - for a child that first appears mid-re-fold
    * (created by a parent's replayed handler) so the `seq <= restoredSeq` filter can skip what its snapshot already
    * holds.
    */
  private def ensureRestored(process: Process[F, ?, ?]): F[Unit] =
    if recovered.contains(process.ref) then effect.pure(())
    else
      recovered.add(process.ref)
      restoreState(process).void

  /** Recovers every child the just-restored `parent` registered while handling [[io.parapet.core.Events.Restored]]. */
  private def recoverChildren(parent: ProcessRef.Unknown): F[Unit] =
    context.child(parent).foldLeft(effect.pure(())) { (acc, childRef) =>
      acc >> context.getProcessState(childRef).fold(effect.pure(()))(ps => recover(ps.process))
    }

  /** Delivers a lifecycle event to `ref` synchronously through the interpreter (not via the scheduler), so its handler
    * runs now. Under [[BootMode.Replaying]] its sends are suppressed, but `register` (child re-spawn) still runs.
    */
  private def deliverLifecycle(ref: ProcessRef.Unknown, event: Event): F[Unit] =
    context.getProcessState(ref) match
      case None     => effect.raiseError[Unit](new IllegalStateException(s"recovery: no process for $ref"))
      case Some(ps) =>
        effect.suspend(ps.process(event).foldMap(interpreter.interpret(ProcessRef.SystemRef, ps, Scope.empty)).void)

  /** Re-folds the recorded history onto the registered processes: each entry replayed to its receiver in `seq` order,
    * skipped when already in the receiver's snapshot (`seq <= restoredSeq`) or the receiver does not exist.
    */
  private def reFold(): F[Unit] =
    context.readJournal(0L).flatMap { entries =>
      entries.foldLeft(effect.pure(()))((acc, entry) => acc >> reFoldEntry(entry))
    }

  private def reFoldEntry(entry: JournalEntry): F[Unit] =
    context.getProcessState(entry.receiver) match
      case Some(ps) =>
        // A child first seen here (created by a parent's replayed handler) is restored from its snapshot before the
        // filter, so entries already folded into that snapshot are skipped rather than re-applied from scratch.
        ensureRestored(ps.process).flatMap { _ =>
          if entry.seq > ps.restoredSeq then replayDeliver(entry) else effect.pure(())
        }
      case None =>
        effect.delay(logger.warn(s"replay: no process for receiver ${entry.receiver} at seq ${entry.seq}; skipping"))

  /** Re-delivers one recorded entry to its receiver: decode the event, then run the receiver's handler through the
    * interpreter under the entry's causal scope. Fails loud if the receiver does not exist or its codec is unknown.
    */
  def replayDeliver(entry: JournalEntry): F[Unit] =
    context.getProcessState(entry.receiver) match
      case None =>
        effect.raiseError[Unit](new IllegalStateException(s"replay: no process for receiver ${entry.receiver}"))
      case Some(ps) =>
        context.codecForTag(entry.tag) match
          case None =>
            effect.raiseError[Unit](new IllegalStateException(s"replay: no codec for tag '${entry.tag}'"))
          case Some(codec) =>
            codec.decode(entry.schemaVersion, entry.event) match
              case Failure(error) => effect.raiseError[Unit](error)
              case Success(event) =>
                val scope = Scope.empty.put(Scope.Cause, entry.id)
                effect.suspend(ps.process(event).foldMap(interpreter.interpret(entry.sender, ps, scope)).void)

  /** Folds a receiver's recorded deliveries in `seq` order (parallelism across receivers is added later). */
  def replay(entries: Vector[JournalEntry]): F[Unit] =
    entries.foldLeft(effect.pure(()))((acc, entry) => acc >> replayDeliver(entry))
