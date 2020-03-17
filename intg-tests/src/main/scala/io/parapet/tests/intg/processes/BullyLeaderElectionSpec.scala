package io.parapet.tests.intg.processes

import com.google.protobuf.ByteString
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.processes.BullyLeaderElection.{Answer, AnswerTimeout, Coordinator, CoordinatorTimeout, Election, Peer, Ready, WaitForCoordinator}
import io.parapet.core.processes.PeerProcess.{Ack, CmdEvent, Reg, Send}
import io.parapet.core.processes.{BullyLeaderElection, PeerProcess}
import io.parapet.core.{Channel, Event, Process}
import io.parapet.p2p.Protocol
import io.parapet.p2p.Protocol.CmdType
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._


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
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Reg(ble.ref))
  }


  test("start election once quorum is full") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _: PeerProcess.Send => ()
    })
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](_ => {
      case Start => Ack("1") ~> ble ++ joined("2") ~> ble
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Send("2", Election(1)))
  }

  test("process is ready - peer left - quorum is full - do nothing") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = Process.unit[F]
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val ch = new Channel[F]()
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Seq(Ack("1"), joined("2"), joined("3"), deliver("3", Coordinator(3)), left("2")) ~> ble ++
          ch.send(BullyLeaderElection.Echo, ble.ref, _ => eval(eventStore.add(ref, BullyLeaderElection.Echo)))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    ble.leader.value shouldBe Peer("3", 3)
  }

  test("process is ready - leader left - start election") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send(_, _: Election) => ()
    })
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
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
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(3), hasher)
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

  test("process with highest id sends Coordinator") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case _: PeerProcess.Send => ()
    })
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(3), hasher)

    val test = Process[F](ref => {
      case Start => Ack("3") ~> ble ++ joined("1") ~> ble ++ joined("2") ~> ble
    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(Send("1", Coordinator(3)), Send("2", Coordinator(3)))

  }

  test("waitForAnswer receive answer switch to waitForCoordinator") {
    val peerProcess = Process.unit[F]
    val eventStore = new EventStore[F, Event]
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
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
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
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


  test("process waitingForAnswer received election sends ok") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("1", Answer(2)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Ack("2") ~> ble ++ joined("3") ~> ble ++ joined("1") ~> ble ++
          deliver("1", Election(1)) ~> ble

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
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Ack("2") ~> ble ++ joined("3") ~> ble ++ joined("1") ~> ble ++
          AnswerTimeout ~> ble

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(PeerProcess.Send("1", Coordinator(2)))

    ble.leader.value shouldBe Peer("2", 2)

  }

  test("process waitingForCoordinator received timeout restarts election") {
    val eventStore = new EventStore[F, Event]
    val peerProcess = createPeerProcess(eventStore, {
      case PeerProcess.Send("2", Election(1)) => ()
    })
    val ch = new Channel[F]()
    val ble = new BullyLeaderElection[F](peerProcess.ref, BullyLeaderElection.Config(2), hasher)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Ack("1") ~> ble ++ joined("2") ~> ble ++
          deliver("2", Answer(2)) ~> ble ++
          CoordinatorTimeout ~> ble

    })

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(test, ble, peerProcess))).run))

    eventStore.get(peerProcess.ref) shouldBe Seq(PeerProcess.Send("2", Election(1)), PeerProcess.Send("2", Election(1)))

    ble.leader shouldBe None

  }


  def createPeerProcess(es: EventStore[F, Event], filter: PartialFunction[Event, Unit]): Process[F] = {
    Process[F](ref => {
      case Start | Stop => unit
      case e => if (filter.isDefinedAt(e))
        eval(es.add(ref, e)) else unit
    })
  }

  def deliver(peerId: String, cmd: BullyLeaderElection.Command): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(peerId).setCmdType(CmdType.DELIVER).setData(ByteString.copyFrom(cmd.marshall)).build())
  }

  def joined(id: String): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(id).setCmdType(CmdType.JOINED).build())
  }

  def left(id: String): Event = {
    CmdEvent(Protocol.Command.newBuilder().setPeerId(id).setCmdType(CmdType.LEFT).build())
  }
}
