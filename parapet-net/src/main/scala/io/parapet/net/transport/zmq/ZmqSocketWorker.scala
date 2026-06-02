package io.parapet.net.transport.zmq

import com.typesafe.scalalogging.Logger
import io.parapet.net.transport.{ReceiveResult, TransportError}
import org.slf4j.LoggerFactory
import org.zeromq.{ZContext, ZMQ, ZMQException}

import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.CancellationException

/** Owns a single non-thread-safe ZMQ socket on one worker thread.
  *
  * ZMQ sockets should be touched by one thread. Callers submit typed commands into the command queue; the worker drains
  * those commands and interprets them against the socket. Inbound socket messages are decoded by `readInbound` and
  * placed into a bounded queue consumed by [[poll]].
  *
  * A full inbound queue applies backpressure by slowing socket reads instead of dropping messages. Malformed/protocol
  * data should be returned as `ReceiveResult.Failed` so the worker can keep running. Exceptions that escape the worker
  * loop are treated as transport failure: pending commands are completed, the context is closed, and future calls
  * observe `Closed`/`Failed`.
  */
final private[zmq] class ZmqSocketWorker[C, A](
    context: ZContext,
    socket: ZMQ.Socket,
    readInbound: ZMQ.Socket => ReceiveResult[A],
    handleCommand: (ZMQ.Socket, C) => Either[TransportError, Unit],
    threadName: String,
    inboundCapacity: Int
):
  import ZmqSocketWorker.{EnqueuePollMs, WorkItem}

  private val logger   = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
  private val commands = new ConcurrentLinkedQueue[WorkItem[C]]()
  private val inbound  = new LinkedBlockingQueue[ReceiveResult[A]](inboundCapacity)
  private val running  = new AtomicBoolean(true)

  private val worker = new Thread(() => runLoop(), threadName)
  worker.setDaemon(true)
  worker.start()

  private def runLoop(): Unit =
    var shutdownError: Option[TransportError] = None
    try
      while running.get() do
        drainCommands()
        if running.get() then
          readInbound(socket) match
            case ReceiveResult.Idle => ()
            case result             => enqueue(result)
    catch
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        val transportError = TransportError.Unexpected(error)
        shutdownError = Some(transportError)
        logger.error("ZMQ socket worker was interrupted. thread: {}", threadName, error)
        inbound.offer(ReceiveResult.Failed(transportError))
      case error: Throwable =>
        val transportError = TransportError.Unexpected(error)
        shutdownError = Some(transportError)
        logger.error("ZMQ socket worker failed. thread: {}", threadName, error)
        inbound.offer(ReceiveResult.Failed(transportError))
    finally
      running.set(false)
      val finalResult =
        shutdownError.fold[Either[TransportError, Unit]](Left(TransportError.Closed("close")))(Left(_))
      var command = commands.poll()
      while command != null do
        command.complete(finalResult)
        command = commands.poll()
      context.close()

  private def drainCommands(): Unit =
    var command = commands.poll()
    while command != null do
      command.run(socket, handleCommand)
      command = commands.poll()

  private def enqueue(result: ReceiveResult[A]): Unit =
    var warned = false
    while running.get() && !inbound.offer(result, EnqueuePollMs, TimeUnit.MILLISECONDS) do
      if !warned then
        logger.warn("inbound queue is full")
        warned = true
      drainCommands()

  def poll(timeoutMs: Int): ReceiveResult[A] =
    Option(inbound.poll()) match
      case Some(result) =>
        result
      case None =>
        if !running.get() then ReceiveResult.Failed(TransportError.Closed("receive"))
        else
          try
            Option(inbound.poll(timeoutMs.toLong, TimeUnit.MILLISECONDS)) match
              case Some(result) =>
                result
              case None =>
                if running.get() then ReceiveResult.Idle
                else ReceiveResult.Failed(TransportError.Closed("receive"))
          catch
            case error: InterruptedException =>
              Thread.currentThread().interrupt()
              ReceiveResult.Failed(TransportError.Unexpected(error))

  def submit(command: C): Either[TransportError, Unit] =
    if !running.get() then Left(TransportError.Closed("submit"))
    else
      val item = new WorkItem(command)
      commands.add(item)
      if !running.get() then item.complete(Left(TransportError.Closed("submit")))
      item.await()

  def close(): Unit =
    running.set(false)
    if Thread.currentThread() ne worker then
      try worker.join()
      catch
        case _: InterruptedException =>
          Thread.currentThread().interrupt()

object ZmqSocketWorker:
  private val EnqueuePollMs = 100L

  final private class WorkItem[C](command: C):
    private val result = new CompletableFuture[Either[TransportError, Unit]]()

    def run(
        socket: ZMQ.Socket,
        handleCommand: (ZMQ.Socket, C) => Either[TransportError, Unit]
    ): Unit =
      val outcome =
        try handleCommand(socket, command)
        catch
          case error: InterruptedException =>
            Thread.currentThread().interrupt()
            Left(TransportError.Unexpected(error))
          case error: ZMQException =>
            Left(TransportError.Unexpected(error))
          case error: Throwable =>
            Left(TransportError.Unexpected(error))
      result.complete(outcome)
      ()

    def complete(outcome: Either[TransportError, Unit]): Unit =
      result.complete(outcome)
      ()

    def await(): Either[TransportError, Unit] =
      try result.get()
      catch
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          Left(TransportError.Unexpected(error))
        case error: ExecutionException =>
          Left(TransportError.Unexpected(Option(error.getCause).getOrElse(error)))
        case error: CancellationException =>
          Left(TransportError.Unexpected(error))
