package io.parapet.tests.intg.processes

import com.google.protobuf.ByteString
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.processes.BullyLeaderElection.{Answer, Coordinator, Election, WaitForCoordinator}
import io.parapet.core.processes.PeerProcess.{Ack, CmdEvent, Reg, Send}
import io.parapet.core.processes.{BullyLeaderElection, PeerProcess}
import io.parapet.core.{Channel, Event, Process}
import io.parapet.p2p.Protocol
import io.parapet.p2p.Protocol.CmdType
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

abstract class BullyLeaderElectionSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

  //private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  import dsl._

  val echoProcess = Process[F](_ => {
    case Echo => withSender(Echo ~> _)
  })

  object Echo extends Event

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
    val blee = ble.or(echoProcess)
    val test = Process[F](ref => {
      case Start =>
        register(ref, ch) ++
          Ack("1") ~> blee ++ joined("2") ~> blee ++
          deliver("2", Answer(2)) ~> blee ++
          ch.send(Echo, blee.ref, _ => eval(eventStore.add(ref, EventStore.Dummy)))

    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, blee, peerProcess))).run))

    ble.state shouldBe WaitForCoordinator
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


}
