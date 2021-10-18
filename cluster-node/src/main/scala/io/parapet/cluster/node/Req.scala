package io.parapet.cluster.node

import io.parapet.Event

case class Req(nodeId: String, data: Array[Byte]) extends Event
