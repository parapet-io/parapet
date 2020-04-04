package io.parapet.tests.intg.processes

import com.google.protobuf.ByteString
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.processes.BullyLeaderElection._
import io.parapet.core.processes.PeerProcess.{Ack, CmdEvent, Reg, Send}
import io.parapet.core.processes.{BullyLeaderElection, PeerProcess}
import io.parapet.core.{Channel, Event, Process, ProcessRef}
import io.parapet.p2p.Protocol
import io.parapet.p2p.Protocol.CmdType
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.util.Success


abstract class BullyLeaderElectionSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

  import dsl._

  def hasher: String => Long = {
    case "1" => 1
    case "2" => 2
    case "3" => 3
  }

  test("first event sent to peer process should be Reg") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _: PeerProcess.Reg => ()
    })
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Reg(ble.ref))
  }

  test("start election once quorum is full") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _: PeerProcess.Send => ()
    })
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](_ => {
      case Start => Seq(Ack("1"), joined("2")) ~> ble
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Send("2", Election(1)))
  }

  test("start election if only one process can exist") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process.unit[F]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(1), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1")) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader.value shouldBe Peer("1", 1)
    ble.state shouldBe Ready
  }

  test("process is ready - peer left - quorum is full - do nothing") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _ => ()
    })
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), joined("3"), deliver("3", Coordinator(3)), left("2")) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(test, ble, peerProcess))).run))
    eventStore.get(peerProcess.ref) shouldBe Seq(Reg(ble.ref), Send("2", Election(1)))
    ble.leader.value shouldBe Peer("3", 3)
  }

  test("process is ready - leader left - start election") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send(_, _: Election) => ()
    })
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"),
            joined("2"),
            joined("3"),
            deliver("3", Coordinator(3)),
            left("3")
          ) ~> ble
    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))
    eventStore.allEvents shouldBe Seq(Send("2", Election(1)), Send("2", Election(1)))
    ble.leader shouldBe None
  }

  test("process is ready - peer left - incomplete quorum - discard leader") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process.unit[F]
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(3), hasher)
    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"),
            joined("2"),
            joined("3"),
            deliver("3", Coordinator(3)),
            left("2")
          ) ~> ble ++ ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))
    ble.leader shouldBe None
  }

  test("process with highest id becomes leader") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _: PeerProcess.Send => ()
    })
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(3), hasher)

    val test = Process[F](_ => {
      case Start => Seq(Ack("3"), joined("1"), joined("2")) ~> ble
    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Send("1", Coordinator(3)), Send("2", Coordinator(3)))
    ble.state shouldBe Ready
  }

  test("waitForAnswer receive answer switch to waitForCoordinator") {
    val peerProcess = Process.unit[F]
    val eventStore = new EventStore[F, Event]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), deliver("2", Answer(2))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.state shouldBe WaitForCoordinator
  }

  test("process waitingForAnswer received Coordinator sets leader") {
    val peerProcess = Process.unit[F]
    val eventStore = new EventStore[F, Event]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), deliver("2", Coordinator(2))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.state shouldBe Ready

    ble.leader.value shouldBe Peer("2", 2)
  }

  test("process waitingForAnswer received Election sends ok") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("1", Answer(2)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("2"), joined("3"), joined("1"), deliver("1", Election(1))) ~> ble
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(PeerProcess.Send("1", Answer(2)))
  }

  test("process waitingForAnswer received timeout elects itself") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("1", Coordinator(2)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("2"), joined("3"), joined("1"), AnswerTimeout) ~> ble
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(PeerProcess.Send("1", Coordinator(2)))

    ble.leader.value shouldBe Peer("2", 2)

  }

  test("process waitingForCoordinator received Coordinator sets leader") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("1", Coordinator(2)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), deliver("2", Coordinator(2))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader.value shouldBe Peer("2", 2)

  }

  test("process waitingForCoordinator received timeout restarts election") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("2", Election(1)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), deliver("2", Answer(2)), CoordinatorTimeout) ~> ble
    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(PeerProcess.Send("2", Election(1)), PeerProcess.Send("2", Election(1)))

    ble.leader shouldBe None

  }

  test("proces sent answer and left - process elects itself") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("1", Coordinator(2)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("2"), joined("1"), joined("3"), deliver("3", Answer(3)), left("3"), CoordinatorTimeout) ~> ble
    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader.value shouldBe Peer("2", 2)
    ble.state shouldBe Ready
  }

  // Scenario:
  // peer-3 joined peer-1
  // peer-1 joined peer-3
  // both started election
  // peer-1 waiting for answer
  // peer-3 sent Coordinator to peer-1
  // peer-1 switched to ready state
  // peer-2 joined peer-3
  // peer-3 sent Coordinator to peer-1, peer-2
  // peer-1 in ready state received Coordinator from peer-3
  test("peer in ready state and elected leader received Coordinator from new election cycle - do nothing") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("2", Election(1)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("3"),
            deliver("3", Coordinator(3)), deliver("3", Coordinator(3))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader.value shouldBe Peer("3", 3)
  }

  test("peer in ready state received Coordinator from unknown process - should fail") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process.unit[F]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++ Seq(Ack("1"), deliver("3", Coordinator(3))) ~> ble
      case f@Event.Failure(_, _) => eval(eventStore.add(ref, f))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader shouldBe None
  }

  // Scenario:
  // peer-1 joined peer-2
  // peer-2 joined peer-1
  // peer-1 started election and waiting for answer
  // peer-2 sent coordinator to peer-1
  // peer-3 joined peer-1
  // peer-3 sent coordinator to peer-1
  // peer-1 received coordinator from peer-2
  // peer-1 switched to ready state
  // peer-1 received coordinator from peer-3
  // peer-1 updates its leader
  test("peer in ready state received Coordinator from new election process from process with highest id") {
    val peerProcess = Process.unit[F]
    val eventStore = new EventStore[F, Event]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"),
            joined("2"),
            joined("3"),
            deliver("2", Coordinator(2)),
            deliver("3", Coordinator(3))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.state shouldBe Ready

    ble.leader.value shouldBe Peer("3", 3)
  }

  // Scenario:
  // peer-1 joined peer-2
  // peer-2 joined peer-1
  // peer-1 started election and waiting for answer
  // peer-2 sent coordinator to peer-1
  // peer-3 joined peer-1
  // peer-3 sent coordinator to peer-1
  // peer-1 received coordinator from peer-3
  // peer-1 switched to ready state
  // peer-1 received coordinator from peer-2
  // peer-1 doesn't update its leader
  test("peer in ready state received Coordinator from delayed process with lower id") {
    val peerProcess = Process.unit[F]
    val eventStore = new EventStore[F, Event]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"),
            joined("2"),
            joined("3"),
            deliver("3", Coordinator(3)),
            deliver("2", Coordinator(2))) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.state shouldBe Ready

    ble.leader.value shouldBe Peer("3", 3)
  }

  test("send req to leader receives response") {
    val eventStore = new EventStore[F, Event]

    val peerProcess = Process(_ => {
      case _ => unit
    })

    val clientProcess = Process[F](_ => {
      case BullyLeaderElection.Req(data) =>
        withSender(s => BullyLeaderElection.Rep(BullyLeaderElection.Ok, ("rep:" + new String(data)).getBytes()) ~> s)
    })

    val ble = new BullyLeaderElection[F](clientProcess.ref, peerProcess.ref, BullyLeaderElection.Config(2), hasher)

    val ch = new Channel[F]()

    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("2"), joined("1")) ~> ble ++
          ch.send(BullyLeaderElection.Req("hi".getBytes()), ble.ref, {
            case Success(res) => eval(eventStore.add(ref, res))
          })
    })
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, peerProcess, clientProcess, ble))).run))

    eventStore.get(test.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Ok, data) if new String(data) == "rep:hi" =>
    }
  }

  test("send req to non-leader forwards request") {
    val eventStore = new EventStore[F, Event]
    // refs
    val peerProcess1Ref = ProcessRef.jdkUUIDRef
    val peerProcess2Ref = ProcessRef.jdkUUIDRef

    // same client code executed by all processes
    val clientProcess = Process[F](_ => {
      case BullyLeaderElection.Req(data) =>
        withSender(s => BullyLeaderElection.Rep(BullyLeaderElection.Ok, ("rep:" + new String(data)).getBytes()) ~> s)
    })

    val ble1 = new BullyLeaderElection[F](clientProcess.ref, peerProcess1Ref, BullyLeaderElection.Config(2), hasher)
    val ble2 = new BullyLeaderElection[F](clientProcess.ref, peerProcess2Ref, BullyLeaderElection.Config(2), hasher)

    val peerProcess1 = Process.builder[F](_ => {
      case PeerProcess.Send("2", req@BullyLeaderElection.ReqWithId(_, 1)) => deliver("1", req) ~> ble2
      case _ => unit
    }).ref(peerProcess1Ref).build

    val peerProcess2 = Process.builder[F](_ => {
      case PeerProcess.Send("1", rep: BullyLeaderElection.Rep) => deliver("2", rep) ~> ble1
      case _ => unit
    }).ref(peerProcess2Ref).build

    val ch = new Channel[F]()

    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"),
            joined("2"),
            deliver("2", Coordinator(2))) ~> ble1 ++
          Seq(Ack("2"), joined("1")) ~> ble2 ++
          ch.send(BullyLeaderElection.Req("hi".getBytes()), ble1.ref, {
            case Success(res) => eval(eventStore.add(ref, res))
          })

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, clientProcess, ble1, ble2, peerProcess1, peerProcess2))).run))

    eventStore.get(test.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Ok, data) if new String(data) == "rep:hi" =>
    }
  }

  test("send req to process without leader should return error") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process(_ => {
      case _ => unit
    })

    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)

    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1")) ~> ble ++
          ch.send(BullyLeaderElection.Req("hi".getBytes()), ble.ref, {
            case Success(res) => eval(eventStore.add(ref, res))
          })
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(test.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Error, data) if new String(data) == "leader is null" =>
    }

  }

  test("peer send req to process without leader should return error") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process(ref => {
      case PeerProcess.Send("2", rep: BullyLeaderElection.Rep) => eval(eventStore.add(ref, rep))
      case _ => unit
    })

    val ble = new BullyLeaderElection[F](ProcessRef.jdkUUIDRef, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    ble.peers.put(2, BullyLeaderElection.Peer("2", 2))
    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1")) ~> ble ++
          deliver("2", BullyLeaderElection.ReqWithId("hi".getBytes(), 2)) ~> ble
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Error, data) if new String(data) == "leader is null" =>
    }
  }

  test("master failed to process req - sender should receive rep") {
    val eventStore = new EventStore[F, Event]

    val peerProcess = Process(_ => {
      case _ => unit
    })

    val clientProcess = Process.builder[F](_ => {
      case BullyLeaderElection.Req(data) => eval(throw new IllegalStateException(s"client failed to process req: ${new String(data)}"))
    }).ref(ProcessRef("clientProcess")).build

    val ble = new BullyLeaderElection[F](clientProcess.ref, peerProcess.ref, BullyLeaderElection.Config(2), hasher)

    val ch = new Channel[F]()

    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("2"), joined("1")) ~> ble ++
          ch.send(BullyLeaderElection.Req("hi".getBytes()), ble.ref, {
            case Success(res) => eval(eventStore.add(ref, res))
          })
    })
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, peerProcess, clientProcess, ble))).run))

    eventStore.get(test.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Error, data) if new String(data).contains("has failed to handle event: Req") =>
    }
  }

  test("peer send req to leader and client failed - reply error") {
    val eventStore = new EventStore[F, Event]
    val clientProcess = Process.builder[F](_ => {
      case BullyLeaderElection.Req(data) => eval(throw new IllegalStateException(s"client failed to process req: ${new String(data)}"))
    }).ref(ProcessRef("clientProcess")).build
    val peerProcess = Process(ref => {
      case PeerProcess.Send("1", rep: BullyLeaderElection.Rep) => eval(eventStore.add(ref, rep))
      case _ => unit
    })
    val ble = new BullyLeaderElection[F](clientProcess.ref, peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    ble._leader = Peer("2", 2)
    ble.peers.put(1, Peer("1", 1))
    val test = Process[F](ref => {
      case Start => Ack("2") ~> ble ++
        deliver("1", ReqWithId("hi".getBytes(), 1)) ~> ble
    })
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, peerProcess, clientProcess, ble))).run))
    eventStore.get(peerProcess.ref).headOption.value should matchPattern {
      case BullyLeaderElection.Rep(BullyLeaderElection.Error, data) if new String(data).contains("has failed to handle event: Req") =>
    }
  }

  test("peer in waitingForAnswer state received join") {
    // todo
  }

  test("peer in waitingForAnswer state received left") {
    // todo
  }

  test("peer in waitingForCoordinator state received join") {
    // todo
  }

  test("peer in waitingForCoordinator state received left") {
    // todo
  }

  def createPeerProcess(es: EventStore[F, Event], filter: PartialFunction[Event, Unit]): Process[F] = {
    Process[F](ref => {
      case Start | Stop => unit
      case e => if (filter.isDefinedAt(e))
        eval(es.add(ref, e)) else unit
    })
  }

  def deliver(sender: String, cmd: BullyLeaderElection.Command): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(sender).setCmdType(CmdType.DELIVER).setData(ByteString.copyFrom(cmd.marshall)).build())
  }

  def joined(id: String): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(id).setCmdType(CmdType.JOINED).build())
  }

  def left(id: String): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(id).setCmdType(CmdType.LEFT).build())
  }

  def rep(id: String): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(id).setCmdType(CmdType.LEFT).build())
  }

}
