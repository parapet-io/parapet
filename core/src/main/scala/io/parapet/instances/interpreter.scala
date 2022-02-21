package io.parapet.instances

import cats.{Id, Monad, ~>, Eval => CatsEval}
import io.parapet.ProcessRef
import io.parapet.core.Dsl._
import io.parapet.core.{EventLog, EventTransformer}

object interpreter {

  class EvalInterpreter(senderRef: ProcessRef,
                        eventLog: EventLog,
                        eventTransformer: EventTransformer = EventTransformer.Noop)
    extends (FlowOp[CatsEval, *] ~> cats.Id) {

    override def apply[A](fa: FlowOp[CatsEval, A]): Id[A] = {
      fa match {
        case f: SuspendF[CatsEval, Dsl[CatsEval, *], A] => f.thunk().foldMap(this)
        case _: UnitFlow[CatsEval]@unchecked => ()
        case eval: Eval[CatsEval, Dsl[CatsEval, *], A]@unchecked =>
          implicitly[Monad[Id]].pure(eval.thunk())
        case send: Send[CatsEval]@unchecked =>
          val event = eventTransformer.transform(send.e())
          eventLog.add(senderRef, event, send.receiver)
          send.receivers.foreach(p => {
            eventLog.add(senderRef, event, p)
          })
        case fork: Fork[CatsEval, Dsl[CatsEval, *]] =>
          fork.flow.foldMap(new EvalInterpreter(senderRef, eventLog, eventTransformer))
        case _: Delay[CatsEval] => ()
        case _: SuspendF[CatsEval, Dsl[CatsEval, *], A] => ().asInstanceOf[A] // s.thunk().foldMap(new IdInterpreter(execution))
        case withSender: WithSender[CatsEval, Dsl[CatsEval, *], A] =>
          withSender.f(senderRef).foldMap(new EvalInterpreter(senderRef, eventLog, eventTransformer))
        case RaiseError(err) => throw err
      }
    }
  }

}
