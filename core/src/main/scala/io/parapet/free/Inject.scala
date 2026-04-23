package io.parapet.free

sealed trait Coproduct[F[_], G[_], A]

object Coproduct:
  final case class Left[F[_], G[_], A](value: F[A]) extends Coproduct[F, G, A]
  final case class Right[F[_], G[_], A](value: G[A]) extends Coproduct[F, G, A]

type :+:[F[_], G[_]] = [A] =>> Coproduct[F, G, A]

trait Inject[F[_], G[_]]:
  def inject[A](fa: F[A]): G[A]

object Inject:
  def apply[F[_], G[_]](using inject: Inject[F, G]): Inject[F, G] = inject

  given identity[F[_]]: Inject[F, F] with
    def inject[A](fa: F[A]): F[A] = fa

  given left[F[_], G[_]]: Inject[F, F :+: G] with
    def inject[A](fa: F[A]): Coproduct[F, G, A] =
      Coproduct.Left(fa)

  given right[F[_], G[_], H[_]](using inject: Inject[F, G]): Inject[F, H :+: G] with
    def inject[A](fa: F[A]): Coproduct[H, G, A] =
      Coproduct.Right(inject.inject(fa))
