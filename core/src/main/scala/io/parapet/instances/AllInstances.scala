package io.parapet.instances

/** Aggregator trait for typeclass instances exposed by parapet.
  *
  * Currently a placeholder - instances live alongside their respective effect types (e.g. [[io.parapet.effect.ParIO]]'s
  * `effect` and `parallel`). Mix in via [[io.parapet.instances.all]] for forward compatibility.
  */
trait AllInstances
