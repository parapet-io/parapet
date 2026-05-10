package io.parapet

import io.parapet.syntax.AllSyntax

/** Catch-all implicits import.
  *
  * `import io.parapet.implicits._` brings every parapet syntax extension into scope. For finer-grained imports prefer
  * [[io.parapet.syntax]].
  */
package object implicits extends AllSyntax
