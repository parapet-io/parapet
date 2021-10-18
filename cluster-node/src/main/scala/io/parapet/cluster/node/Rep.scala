package io.parapet.cluster.node

import io.parapet.Event

case class Rep(nodeId: String, data: Array[Byte]) extends Event
