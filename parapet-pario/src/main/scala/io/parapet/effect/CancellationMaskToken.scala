package io.parapet.effect

final private[effect] class CancellationMaskToken private ()

private[effect] object CancellationMaskToken:
  def create(): CancellationMaskToken =
    new CancellationMaskToken()
