package io.parapet.core

import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.*
import io.parapet.core.Scheduler.{Deliver, ProcessQueueIsFull}
import io.parapet.effect.{Deferred, Effect}
import io.parapet.effect.Monad.*
import io.parapet.free.{FunctionK, ~>}
import io.parapet.{Envelope, Event, ProcessRef}

/** Translates parapet [[Dsl.FlowOp]] programs into the user's effect type `F[_]`.
  *
  * The interpreter is the bridge between the abstract DSL and a concrete runtime: every operation in the algebra is
  * mapped to an `F`-effect and all routing/scheduling work goes through the surrounding [[Context]] and [[Scheduler]].
  *
  * Custom interpreters can be plugged in via [[io.parapet.ParApp.interpreter]] for cases such as tracing or recording.
  */
object DslInterpreter:
  /** A natural transformation from `FlowOp[F, *]` to `F[*]`. Three overloads adapt the call-site to whatever target
    * identification is convenient (raw ref vs. resolved [[ProcessState]]).
    */
  trait Interpreter[F[_]]:
    /** Interprets ops in the context of `target`, allocating a fresh trace. */
    def interpret(sender: ProcessRef, target: ProcessRef): ([x] =>> FlowOp[F, x]) ~> F

    /** Interprets ops in the context of `target` reusing `execTrace` for causal id. */
    def interpret(sender: ProcessRef, target: ProcessRef, execTrace: ExecutionTrace): ([x] =>> FlowOp[F, x]) ~> F

    /** Interprets ops directly against a known [[ProcessState]] - the form used by the scheduler hot path so it can
      * avoid an extra registry lookup.
      */
    def interpret(
        sender: ProcessRef,
        processState: ProcessState[F],
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F

  /** Builds the default [[Impl]] interpreter bound to `context`. */
  def apply[F[_]](context: Context[F])(using effect: Effect[F]): Interpreter[F] =
    Impl(context)

  /** Default [[Interpreter]] implementation. Delegates routing to [[Context.schedule]] and applies any registered
    * [[EventTransformer]] before enqueueing.
    */
  final class Impl[F[_]](context: Context[F])(using effect: Effect[F]) extends Interpreter[F]:
    def interpret(sender: ProcessRef, target: ProcessRef): ([x] =>> FlowOp[F, x]) ~> F =
      interpret(sender, target, context.createTrace)

    def interpret(sender: ProcessRef, target: ProcessRef, execTrace: ExecutionTrace): ([x] =>> FlowOp[F, x]) ~> F =
      interpret(sender, context.getProcessState(target).get, execTrace)

    def interpret(
        sender: ProcessRef,
        processState: ProcessState[F],
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F =
      new FunctionK[[x] =>> FlowOp[F, x], F]:
        def apply[A](fa: FlowOp[F, A]): F[A] =
          fa match
            case Dsl.Pure(value) =>
              effect.pure(value).asInstanceOf[F[A]]

            case UnitFlow() =>
              effect.pure(()).asInstanceOf[F[A]]

            case Send(event, senderOverride, receiver, receivers) =>
              val source = senderOverride.getOrElse(processState.process.ref)
              val first  = send(source, event, receiver, execTrace)
              if receivers.nonEmpty then
                (first >> receivers.foldLeft(effect.pure(())) { (acc, next) =>
                  acc >> send(source, event, next, execTrace)
                }).asInstanceOf[F[A]]
              else first.asInstanceOf[F[A]]

            case WithSender(runWithSender) =>
              runWithSender
                .asInstanceOf[ProcessRef => DslF[F, A]]
                .apply(sender)
                .foldMap(interpret(sender, processState, execTrace))

            case Forward(event, receivers) =>
              receivers
                .foldLeft(effect.pure(())) { (acc, receiver) =>
                  acc >> send(sender, event, receiver, execTrace)
                }
                .asInstanceOf[F[A]]

            case Par(flow) =>
              flow
                .asInstanceOf[DslF[F, Unit]]
                .foldMap(interpret(sender, processState, execTrace))
                .asInstanceOf[F[A]]

            case Fork(flow) =>
              effect
                .start(
                  flow
                    .asInstanceOf[DslF[F, A]]
                    .foldMap(interpret(sender, processState, execTrace))
                )
                .map(fiber => Fiber.RuntimeFiber(fiber).asInstanceOf[A])

            case delay: Delay[F] =>
              effect.sleep(delay.duration).asInstanceOf[F[A]]

            case Eval(thunk) =>
              effect.delay(thunk().asInstanceOf[A])

            case Suspend(thunk) =>
              effect.suspend(thunk().asInstanceOf[F[A]])

            case SuspendF(thunk) =>
              effect
                .suspend(
                  thunk()
                    .asInstanceOf[DslF[F, A]]
                    .foldMap(interpret(sender, processState, execTrace))
                )

            case Race(first, second) =>
              val first0  = first.asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace))
              val second0 = second.asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace))
              effect.race(first0, second0).asInstanceOf[F[A]]

            case Blocking(body) =>
              for
                done  <- Deferred[F, Unit]()
                fiber <- effect.start(
                  body()
                    .asInstanceOf[DslF[F, Any]]
                    .foldMap(interpret(sender, processState, execTrace))
                    .flatMap(_ => done.complete(()).void)
                )
                _ <- processState.blocking.add(fiber, done)
              yield ().asInstanceOf[A]

            case Register(parent, process: Process[F] @unchecked) =>
              context.registerAndStart(parent, process).void.asInstanceOf[F[A]]

            case RaiseError(error) =>
              effect.raiseError(error)

            case HandleError(body, onError) =>
              body().asInstanceOf[DslF[F, A]].foldMap(interpret(sender, processState, execTrace)).handleErrorWith {
                error =>
                  onError(error).asInstanceOf[DslF[F, A]].foldMap(interpret(sender, processState, execTrace))
              }

            case Halt(ref) =>
              context.remove(ref).void.asInstanceOf[F[A]]

            case Guarantee(body, finalizer) =>
              effect
                .guarantee(body().asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace))) {
                  finalizer().asInstanceOf[DslF[F, Unit]].foldMap(interpret(sender, processState, execTrace))
                }
                .void
                .asInstanceOf[F[A]]

            case Dsl.Lock(ref) =>
              context.getProcessState(ref).get.acquire.void.asInstanceOf[F[A]]

            case Dsl.Unlock(ref) =>
              (context.getProcessState(ref).get.release >>
                context
                  .schedule(
                    Scheduler.Deliver(Envelope(ProcessRef.SystemRef, Scheduler.Inbox, ref), execTrace)
                  )
                  .void).asInstanceOf[F[A]]

    private def send(
        sender: ProcessRef,
        eventThunk: () => Event,
        receiver: ProcessRef,
        execTrace: ExecutionTrace
    ): F[Unit] =
      effect.suspend {
        val event = context.eventTransformers.get(receiver) match
          case Some(transformer) => transformer.transform(eventThunk())
          case None              => eventThunk()

        val envelope = Envelope(sender, event, receiver)
        context.addToEventLog(envelope) >>
          context.schedule(Deliver(envelope, execTrace.add(envelope.id))).flatMap {
            case ProcessQueueIsFull => context.eventStore.write(envelope)
            case _                  => effect.pure(())
          }
      }
