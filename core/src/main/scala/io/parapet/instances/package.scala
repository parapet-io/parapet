package io.parapet

/** Typeclass instance namespaces.
  *
  * Use [[instances.all]] to import everything; future-facing aggregator that currently holds only the [[AllInstances]]
  * marker.
  */
package object instances {

  /** All-in-one instance import. */
  object all extends AllInstances

  // object parallel extends ParallelInstances

}
