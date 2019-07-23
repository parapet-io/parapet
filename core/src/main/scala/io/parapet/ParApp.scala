package io.parapet

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import cats.~>
import io.parapet.core.Dsl.FlowOps._
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.ProcessRef.SystemRef
import io.parapet.core.processes.{DeadLetterProcess, SystemProcess}
import io.parapet.core.{Context, EventLog, Parallel, Parapet, Process, ProcessRef, Scheduler}

import scala.language.{higherKinds, implicitConversions, reflectiveCalls}

abstract class ParApp[F[_]] {


  type FlowOp[A] = io.parapet.core.Dsl.FlowOp[F, A]
  type Flow[A] = io.parapet.core.DslInterpreter.Flow[F, A]
  type Program = DslF[F, Unit]

  val config: ParConfig = Parapet.defaultConfig

  implicit val parallel: Parallel[F]
  implicit val timer: Timer[F]
  implicit val ct: Concurrent[F]
  implicit val contextShift: ContextShift[F]

  val eventLog: EventLog[F] = EventLog.stub

  def processes: F[Seq[Process[F]]]


  // system processes
  def deadLetter: DeadLetterProcess[F] = DeadLetterProcess.logging

  private[parapet] lazy val systemProcess: Process[F] = new SystemProcess[F]

  def flowInterpreter(context: Context[F]): FlowOp ~> Flow

  val program: Program = implicitly[FlowOps[F, Dsl[F, ?]]].empty

  def unsafeRun(f: F[Unit]): Unit

  //  todo remove
  def stop: F[Unit]

  def run: F[Unit] = {
      for {
        ps <- processes
        _ <- (if (ps.isEmpty) {
          ct.raiseError(new RuntimeException("Initialization error:  at least one process must be provided"))
        } else ct.unit)
        context <- Context(config, eventLog)
        _ <- context.init
        _ <- context.registerAll(ProcessRef.SystemRef, ps.toList :+ deadLetter)
        interpreter <- ct.pure(flowInterpreter(context))
        scheduler <- Scheduler.apply[F](config.schedulerConfig, context, interpreter)
        _ <- parallel.par(
          Seq(interpret_(program, interpreter, FlowState(SystemRef, SystemRef)), scheduler.run))
        _ <- stop
      } yield ()

  }

  def main(args: Array[String]): Unit = {
    unsafeRun(run)
  }
}