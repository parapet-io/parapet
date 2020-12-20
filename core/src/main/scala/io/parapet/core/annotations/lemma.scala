package io.parapet.core.annotations

import io.parapet.core.doc.Lemma

class lemma[T <: Lemma](name: String = "") extends scala.annotation.StaticAnnotation {
  def this(clazz: Class[T]) = this("")
}
