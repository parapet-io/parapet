package io.parapet

/** Syntax extensions and convenience importable namespaces.
  *
  * Use [[syntax.all]] to import every available extension method, or [[syntax.logger]] to
  * cherry-pick the MDC helpers.
  */
package object syntax {

  /** All-in-one syntax import: brings every extension method into scope. */
  object all extends AllSyntax

  /** Just the [[LoggerSyntax]] extensions. */
  object logger extends LoggerSyntax

}
