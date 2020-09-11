package io.parapet

package object monixinstances {
  object all extends ParallelInstances with ParAsyncInstances
  object parallel extends ParallelInstances
}
