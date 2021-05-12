package io.parapet.cluster.node

import io.parapet.core.Event

case class Rep(nodeId: String, data: Array[Byte]) extends Event
