package io.parapet.cluster.node

import io.parapet.core.api.Event

case class Rep(nodeId: String, data: Array[Byte]) extends Event
