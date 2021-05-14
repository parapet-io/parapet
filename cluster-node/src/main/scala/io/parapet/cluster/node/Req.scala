package io.parapet.cluster.node

import io.parapet.core.Event

case class Req(nodeId: String, data: Array[Byte]) extends Event
