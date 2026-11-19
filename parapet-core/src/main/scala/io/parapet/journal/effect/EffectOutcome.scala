package io.parapet.journal.effect

sealed trait EffectOutcome

object EffectOutcome:
  case class Success(result: Array[Byte]) extends EffectOutcome
  case class Failure(data: FailureData)   extends EffectOutcome
  case class Cancelled()                  extends EffectOutcome
