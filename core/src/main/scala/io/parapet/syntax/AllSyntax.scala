package io.parapet.syntax

/** Aggregated mix-in of every syntax module exposed by parapet.
  *
  * Mix into a class — or import via `io.parapet.syntax.all` — to bring all extension
  * methods into scope at once.
  */
trait AllSyntax extends LoggerSyntax
