package io.parapet.free

/** Tagged-union over two algebras. Lets a single [[Free]] program mix operations from `F` and `G`.
  */
sealed trait Coproduct[F[_], G[_], A]

/** Constructors for [[Coproduct]]. */
object Coproduct:
  /** Wraps an `F` operation. */
  final case class Left[F[_], G[_], A](value: F[A]) extends Coproduct[F, G, A]

  /** Wraps a `G` operation. */
  final case class Right[F[_], G[_], A](value: G[A]) extends Coproduct[F, G, A]

/** Type alias for a [[Coproduct]] applied to a single `A`. */
type :+:[F[_], G[_]] = [A] =>> Coproduct[F, G, A]

/** Witness that algebra `F` can be embedded into a (possibly larger) algebra `G`.
  *
  * The standard [[Inject.given]] derivations cover the common cases: `F` injects into itself, into the left of a
  * coproduct, and recursively into the right of a coproduct.
  */
trait Inject[F[_], G[_]]:
  /** Lifts `fa: F[A]` into the larger algebra `G[A]`. */
  def inject[A](fa: F[A]): G[A]

/** Built-in [[Inject]] derivations. */
object Inject:
  /** Summons an [[Inject]] instance. */
  def apply[F[_], G[_]](using inject: Inject[F, G]): Inject[F, G] = inject

  /** Trivial: `F` embeds into itself. */
  given identity[F[_]]: Inject[F, F] with
    def inject[A](fa: F[A]): F[A] = fa

  /** `F` embeds into the left side of `F :+: G`. */
  given left[F[_], G[_]]: Inject[F, F :+: G] with
    def inject[A](fa: F[A]): Coproduct[F, G, A] =
      Coproduct.Left(fa)

  /** Recursive case: if `F` injects into `G`, then it injects into `H :+: G` via the right slot.
    */
  given right[F[_], G[_], H[_]](using inject: Inject[F, G]): Inject[F, H :+: G] with
    def inject[A](fa: F[A]): Coproduct[H, G, A] =
      Coproduct.Right(inject.inject(fa))
