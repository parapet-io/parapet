package io.parapet

package object zioinstances {
  object all extends ParallelInstances with ParAsyncInstances
  object parallel extends ParallelInstances
}
