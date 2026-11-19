package io.parapet.journal.effect

final case class EffectKey(deliverySeq: Long, path: List[PathSegment])

final case class PathSegment(id: Int, kind: PathSegment.Kind)

object PathSegment:
  enum Kind:
    case Suspend, Fork, Race, RaceLeft, RaceRight
