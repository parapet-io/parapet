package io.parapet.spark

case class ClusterInfo(address: String,
                       servers: List[String],
                       workers: List[String])
