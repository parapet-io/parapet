package io.parapet.cluster.node

trait MessageHandler {

  def handle(req: Req): Unit

}
