package io.parapet

/** A typed metadata.
  *
  * Each entry is identified by a [[Scope.Key]] which fixes the value type at compile time, so reading an entry returns
  * the exact type the writer put in. `Scope` is an immutable, persistent map; [[Scope.put]] returns a new value sharing
  * structure with the original.
  */
opaque type Scope = Map[String, Any]

object Scope:

  /** The scope with no entries. */
  val empty: Scope = Map.empty

  /** Typed key for a scope entry.
    *
    * @tparam A
    *   the value type associated with this key.
    */
  trait Key[A]:
    /** Globally unique identifier; serves as the underlying map key. */
    def name: String

  /** Identifier of a request that a subsequent message is the response to. */
  case object Causation extends Key[String]:
    val name: String = "io.parapet.causation"

  extension (scope: Scope)
    /** Value associated with `key`, or `None` if absent. */
    def get[A](key: Key[A]): Option[A] =
      scope.get(key.name).asInstanceOf[Option[A]]

    /** A new scope with `key -> value` added or replaced. */
    def put[A](key: Key[A], value: A): Scope =
      scope.updated(key.name, value)

    /** A new scope without `key`. */
    def remove(key: Key[?]): Scope =
      scope - key.name

    /** True when this scope has no entries. */
    def isEmpty: Boolean =
      scope.isEmpty

    /** Underlying name->value pairs. */
    def entries: Iterable[(String, Any)] =
      scope
