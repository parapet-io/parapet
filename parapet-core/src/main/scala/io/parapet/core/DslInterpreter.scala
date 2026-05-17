package io.parapet.core

import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.*
import io.parapet.core.Scheduler.{Deliver, ProcessQueueIsFull}
import io.parapet.effect.{Deferred, Effect}
import io.parapet.effect.Monad.*
import io.parapet.free.{FunctionK, ~>}
import io.parapet.{Envelope, Event, ProcessRef, Scope}

/** Translates parapet [[Dsl.FlowOp]] programs into the user's effect type `F[_]`.
  *
  * The interpreter is the bridge between the abstract DSL and a concrete runtime: every operation in the algebra is
  * mapped to an `F`-effect and all routing/scheduling work goes through the surrounding [[Context]] and [[Scheduler]].
  *
  * Custom interpreters can be plugged in via [[io.parapet.ParApp.interpreter]] for cases such as tracing or recording.
  */
object DslInterpreter:
  /** A natural transformation from `FlowOp[F, *]` to `F[*]`. Several overloads adapt the call-site to whatever target
    * identification is convenient (raw ref vs. resolved [[ProcessState]]).
    */
  trait Interpreter[F[_]]:
    /** Interprets ops in the context of `target`, allocating a fresh trace and empty scope. */
    def interpret(sender: ProcessRef.Unknown, target: ProcessRef.Unknown): ([x] =>> FlowOp[F, x]) ~> F

    /** Interprets ops in the context of `target` reusing `execTrace` for causal id. */
    def interpret(
        sender: ProcessRef.Unknown,
        target: ProcessRef.Unknown,
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F

    /** Interprets ops directly against a known [[ProcessState]] with an empty scope. */
    def interpret(
        sender: ProcessRef.Unknown,
        processState: ProcessState[F],
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F

    /** Interprets ops against a known [[ProcessState]] starting from `scope`. */
    def interpret(
        sender: ProcessRef.Unknown,
        processState: ProcessState[F],
        execTrace: ExecutionTrace,
        scope: Scope
    ): ([x] =>> FlowOp[F, x]) ~> F

  /** Builds the default [[Impl]] interpreter bound to `context`. */
  def apply[F[_]](context: Context[F])(using effect: Effect[F]): Interpreter[F] =
    Impl(context)

  /** Default [[Interpreter]] implementation. Delegates routing to [[Context.schedule]] and applies any registered
    * [[EventTransformer]] before enqueueing.
    */
  final class Impl[F[_]](context: Context[F])(using effect: Effect[F]) extends Interpreter[F]:
    def interpret(sender: ProcessRef.Unknown, target: ProcessRef.Unknown): ([x] =>> FlowOp[F, x]) ~> F =
      interpret(sender, target, context.createTrace)

    def interpret(
        sender: ProcessRef.Unknown,
        target: ProcessRef.Unknown,
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F =
      interpret(sender, context.getProcessState(target).get, execTrace, Scope.empty)

    def interpret(
        sender: ProcessRef.Unknown,
        processState: ProcessState[F],
        execTrace: ExecutionTrace
    ): ([x] =>> FlowOp[F, x]) ~> F =
      interpret(sender, processState, execTrace, Scope.empty)

    def interpret(
        sender: ProcessRef.Unknown,
        processState: ProcessState[F],
        execTrace: ExecutionTrace,
        scope: Scope
    ): ([x] =>> FlowOp[F, x]) ~> F =
      new FunctionK[[x] =>> FlowOp[F, x], F]:
        def apply[A](fa: FlowOp[F, A]): F[A] =
          fa match
            case Dsl.Pure(value) =>
              effect.pure(value)

            case UnitFlow() =>
              effect.pure(())

            case Send(event, senderOverride, receiver, receivers) =>
              val source = senderOverride.getOrElse(processState.process.ref)
              val first  = send(source, event, receiver, execTrace, scope)
              if receivers.nonEmpty then
                first >> receivers.foldLeft(effect.pure(())) { (acc, next) =>
                  acc >> send(source, event, next, execTrace, scope)
                }
              else first

            case WithSender(runWithSender) =>
              runWithSender
                .asInstanceOf[ProcessRef[Event] => DslF[F, A]]
                .apply(sender.asInstanceOf[ProcessRef[Event]])
                .foldMap(interpret(sender, processState, execTrace, scope))

            case Forward(event, receivers) =>
              receivers
                .foldLeft(effect.pure(())) { (acc, receiver) =>
                  acc >> send(sender, event, receiver, execTrace, scope)
                }

            case Par(flow) =>
              flow
                .asInstanceOf[DslF[F, Unit]]
                .foldMap(interpret(sender, processState, execTrace, scope))

            case Fork(flow) =>
              effect
                .start(
                  flow
                    .asInstanceOf[DslF[F, A]]
                    .foldMap(interpret(sender, processState, execTrace, scope))
                )
                .map(fiber => Fiber.RuntimeFiber(fiber).asInstanceOf[A])

            case delay: Delay[F] =>
              effect.sleep(delay.duration)

            case Eval(thunk) =>
              effect.delay(thunk())

            case Suspend(thunk) =>
              effect.suspend(thunk())

            case SuspendF(thunk) =>
              effect
                .suspend(
                  thunk()
                    .asInstanceOf[DslF[F, A]]
                    .foldMap(interpret(sender, processState, execTrace, scope))
                )

            case Race(first, second) =>
              val first0  = first.asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace, scope))
              val second0 = second.asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace, scope))
              effect.race(first0, second0).asInstanceOf[F[A]]

            case Offload(body) =>
              for
                done  <- Deferred[F, Either[Throwable, Unit]]()
                fiber <- effect.startBlocking(
                  body()
                    .asInstanceOf[DslF[F, Any]]
                    .foldMap(interpret(sender, processState, execTrace, scope))
                    .map(_ => Right(()))
                    .handleErrorWith(error => effect.pure(Left(error)))
                    .flatMap(outcome => done.complete(outcome).void)
                )
                _ <- processState.offloads.add(fiber, done)
              yield ().asInstanceOf[A]

            case Register(parent, process: Process[F, ?, ?] @unchecked) =>
              context.registerAndStart(parent, process).void

            case RaiseError(error) =>
              effect.raiseError(error)

            case HandleError(body, onError) =>
              body()
                .asInstanceOf[DslF[F, A]]
                .foldMap(interpret(sender, processState, execTrace, scope))
                .handleErrorWith { error =>
                  onError(error).asInstanceOf[DslF[F, A]].foldMap(interpret(sender, processState, execTrace, scope))
                }

            case Halt(ref) =>
              context.remove(ref).void

            case Guarantee(body, finalizer) =>
              effect
                .guarantee(
                  body().asInstanceOf[DslF[F, Any]].foldMap(interpret(sender, processState, execTrace, scope))
                ) {
                  finalizer()
                    .asInstanceOf[DslF[F, Unit]]
                    .foldMap(interpret(sender, processState, execTrace, scope))
                }
                .void

            case Dsl.Lock(ref) =>
              context.getProcessState(ref).get.acquire.void

            case Dsl.Unlock(ref) =>
              context.getProcessState(ref).get.release >>
                context
                  .schedule(
                    Scheduler.Deliver(Envelope(ProcessRef.SystemRef, Scheduler.Inbox, ref), execTrace)
                  )
                  .void

            case WithScope(f) =>
              f.asInstanceOf[Scope => DslF[F, A]]
                .apply(scope)
                .foldMap(interpret(sender, processState, execTrace, scope))

            case MapScope(f, body) =>
              body
                .asInstanceOf[DslF[F, A]]
                .foldMap(interpret(sender, processState, execTrace, f(scope)))

    private def send(
        sender: ProcessRef.Unknown,
        eventThunk: () => Event,
        receiver: ProcessRef.Unknown,
        execTrace: ExecutionTrace,
        scope: Scope
    ): F[Unit] =
      effect.suspend {
        val event = context.eventTransformers.get(receiver) match
          case Some(transformer) => transformer.transform(eventThunk())
          case None              => eventThunk()

        val envelope = Envelope(sender, event, receiver, scope)
        context.addToEventLog(envelope) >>
          context.schedule(Deliver(envelope, execTrace.add(envelope.id))).flatMap {
            case ProcessQueueIsFull => context.eventStore.write(envelope)
            case _                  => effect.pure(())
          }
      }
