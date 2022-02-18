package io.parapet.spark

import io.parapet.ProcessRef
import io.parapet.cluster.node.NodeProcess
import io.parapet.core.Dsl.DslF
import io.parapet.core.api.Cmd.netServer
import io.parapet.spark.Api.MapResult

class Router[F[_]](clusterMode: Boolean, in: ProcessRef, out: ProcessRef) extends io.parapet.core.Process[F] {
  override val ref: ProcessRef = ProcessRef("router")

  override def handle: Receive = {
    case mr: MapResult => sendToOut(mr.clientId.underlying, mr.toByteArray)
    case NodeProcess.Req(_, data) => Api(data) ~> in
    case netServer.Message(_, data) => Api(data) ~> in
  }

  def sendToOut(clientId: String, data: Array[Byte]): DslF[F, Unit] = {
    if (clusterMode) NodeProcess.Req(clientId, data) ~> out
    else netServer.Send(clientId, data) ~> out
  }
}
