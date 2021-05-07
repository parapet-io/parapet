package io.parapet.cluster.cli

trait MessageHandler {

  def handle(req: Req): Unit

}
