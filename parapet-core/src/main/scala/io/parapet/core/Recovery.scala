package io.parapet.core

import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Events.{Initialize, Registered, Restored}
import io.parapet.core.journal.JournalEntry
import io.parapet.core.snapshot.{Snapshot, Snapshotable}
import io.parapet.effect.Effect
import io.parapet.effect.Monad.*
import io.parapet.{Envelope, Event, ProcessRef, Scope}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/** Restores process state and replays recorded deliveries during application boot. */
final class Recovery[F[_]](context: Context[F], interpreter: Interpreter[F])(using effect: Effect[F]):

  private val logger = LoggerFactory.getLogger(classOf[Recovery[_]])

  private val recovered = scala.collection.mutable.Set.empty[ProcessRef.Unknown]

  // tracks seq per process
  private val processSeqTracker = scala.collection.mutable.Map.empty[ProcessRef.Unknown, Long]

  /** Advances the delivery-sequence and envelope-id counters past what the journal already holds. */
  def seedSequencers(): F[Unit] =
    for
      maxSeq <- context.journalMaxSeq
      maxId  <- context.journalMaxEnvelopeId
      _      <- effect.delay {
        maxSeq.foreach(context.continueSeqAfter)
        maxId.foreach(Envelope.continueIdAfter)
      }
    yield ()

  /** Registers and recovers the initial processes, then replays the journal. */
  def boot(processes: List[Process[F, ?, ?]]): F[Unit] =
    processes.foldLeft(effect.pure(()))((acc, process) =>
      acc >> context.register(ProcessRef.SystemRef, process).void
    ) >>
      processes.foldLeft(effect.pure(()))((acc, process) => acc >> recover(process)) >>
      replay() >>
      recoverPending()

  /** Restores or initializes one process, then recovers the children it registered. */
  private def recover(process: Process[F, ?, ?]): F[Unit] =
    if recovered.contains(process.ref) then effect.pure(())
    else
      recovered.add(process.ref)
      restoreState(process).flatMap {
        case true  => deliverLifecycle(process.ref, Restored)
        case false => deliverLifecycle(process.ref, Initialize)
      } >> recoverChildren(process.ref)

  /** Restores `process` from its latest snapshot and records its replay boundary. */
  private def restoreState(process: Process[F, ?, ?]): F[Boolean] =
    val ref: ProcessRef.Unknown = process.ref
    process match
      case snapshotable: Snapshotable =>
        context.snapshots.fold(effect.pure(Option.empty[Snapshot]))(_.restoreLatest(ref, snapshotable)).flatMap {
          case Some(restored) =>
            effect
              .delay {
                context.continueSeqAfter(restored.metadata.seq)
                context.getProcessState(ref).foreach(s => updateProcessSeq(ref, restored.metadata.seq))
              }
              .map(_ => true)
          case None => effect.pure(false)
        }
      case _ => effect.pure(false)

  private def recoverChildren(parent: ProcessRef.Unknown): F[Unit] =
    context.child(parent).foldLeft(effect.pure(())) { (acc, childRef) =>
      acc >> context.getProcessState(childRef).fold(effect.pure(()))(state => recover(state.process))
    }

  private def recoverPending(): F[Unit] =
    val pending = context.getProcesses.filterNot(process => recoveredOrRuntime(process.ref))
    if pending.isEmpty then effect.pure(())
    else
      pending.foldLeft(effect.pure(()))((acc, process) => acc >> recover(process)) >>
        recoverPending()

  private def recoveredOrRuntime(ref: ProcessRef.Unknown): Boolean =
    recovered.contains(ref) || ProcessRef.RuntimeProcessRefs.contains(ref)

  private def deliverLifecycle(ref: ProcessRef.Unknown, event: Event): F[Unit] =
    context.getProcessState(ref) match
      case None        => effect.raiseError(new IllegalStateException(s"recovery: no process for $ref"))
      case Some(state) =>
        effect.delay(state.process.canHandle(event)).flatMap {
          case false => effect.pure(())
          case true  =>
            effect.suspend(
              state.process(event).foldMap(interpreter.interpret(ProcessRef.SystemRef, state, Scope.empty)).void
            )
        }

  private def replay(): F[Unit] =
    context.readJournal(0L).flatMap { entries =>
      entries.foldLeft(effect.pure(()))((acc, entry) => acc >> replay(entry))
    }

  private def replay(entry: JournalEntry): F[Unit] =
    context.getProcessState(entry.receiver) match
      case Some(_) if !recoveredOrRuntime(entry.receiver) =>
        effect.raiseError(
          new IllegalStateException(
            s"replay: process ${entry.receiver} received entry ${entry.seq} before recovery"
          )
        )
      case Some(state) =>
        if entry.seq > processSeq(state.process.ref)
        then replayDeliver(entry)
        else effect.delay(logger.warn(s"replay ${entry.seq} <= ${processSeq(state.process.ref)}"))
      case None =>
        effect.raiseError(
          new IllegalStateException(s"replay: no process for receiver ${entry.receiver} at seq ${entry.seq}")
        )

  /** Re-delivers one recorded entry without applying snapshot-boundary filtering. */
  private def replayDeliver(entry: JournalEntry): F[Unit] =
    context.getProcessState(entry.receiver) match
      case None => effect.raiseError(new IllegalStateException(s"replay: no process for receiver ${entry.receiver}"))
      case Some(state) =>
        context.codecForTag(entry.tag) match
          case None        => effect.raiseError(new IllegalStateException(s"replay: no codec for tag '${entry.tag}'"))
          case Some(codec) =>
            codec.decode(entry.schemaVersion, entry.event) match
              case Failure(error)                                           => effect.raiseError(error)
              case Success(Registered(child))                               => recoverRegistered(entry, child)
              case Success(_) if state.process.isInstanceOf[ReplayBoundary] => effect.pure(())
              case Success(event)                                           =>
                val scope = Scope.empty.put(Scope.Cause, entry.id)
                effect.suspend(state.process(event).foldMap(interpreter.interpret(entry.sender, state, scope)).void)

  private def recoverRegistered(entry: JournalEntry, child: ProcessRef.Unknown): F[Unit] =
    if entry.sender != ProcessRef.SystemRef then
      effect.raiseError(
        new IllegalStateException(s"replay: registration marker at seq ${entry.seq} has sender ${entry.sender}")
      )
    else if !context.child(entry.receiver).contains(child) then
      effect.raiseError(
        new IllegalStateException(
          s"replay: parent ${entry.receiver} did not recreate child $child before registration marker ${entry.seq}"
        )
      )
    else
      context.getProcessState(child) match
        case Some(state) => recover(state.process)
        case None        =>
          effect.raiseError(new IllegalStateException(s"replay: registered child $child does not exist"))

  private def processSeq(ref: ProcessRef.Unknown): Long = processSeqTracker.getOrElse(ref, 0L)

  private def updateProcessSeq(ref: ProcessRef.Unknown, seq: Long): Unit =
    processSeqTracker.updateWith(ref) {
      case Some(value) => if seq < value then throw new IllegalStateException(s"seq=$seq < $value") else Some(seq)
      case None        => Some(seq)
    }
