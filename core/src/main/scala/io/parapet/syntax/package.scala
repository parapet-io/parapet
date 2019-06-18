package io.parapet

package object syntax {

  object all extends AllSyntax

  object process extends ProcessSyntax
  
  object event extends EventSyntax

  object flow extends FlowSyntax

  object effect extends EffectSyntax

  object boolean {

    implicit class BoolOps(val self: Boolean) extends AnyVal {
      def toOption[A](value: => A): Option[A] =
        if (self) Some(value) else None
    }

  }

}
