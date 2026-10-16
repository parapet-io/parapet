# Event codecs

EventCodec is the public type accepted by the runtime. Clients implement
EventCodec.Simple for one schema identity and use EventCodec.union to compose
existing codecs.

## Implementing a codec

```scala
import io.parapet.{Event, EventCodec}
import io.parapet.EventCodec.Schema
import scala.util.Try

case class Ping() extends Event

object PingCodec extends EventCodec.Simple[Ping] {
  val schema = Schema.Simple("example", "ping")
  val version = 1

  def encode(value: Ping): Try[Array[Byte]] =
    Try(Array.emptyByteArray)

  def decode(version: Int, bytes: Array[Byte]): Try[Ping] = Try {
    require(version == 1 && bytes.isEmpty)
    Ping()
  }
}
```

Clients implement payload encoding and version-aware decoding. The library adds
schema and version metadata and checks schema identity before decoding.
EncodedEvent, encodeEvent, and decodeEvent are private to the parapet package.
Domain events do not need serialization metadata.

A Simple codec represents one schema identity; its payload can distinguish
multiple event variants. The version is the format written by encode. Decode
must handle supported historical versions and reject unsupported versions.
Keep schema identity stable across class renames and wire-layout evolution;
read older layouts into the current model. Use a new identity for a different
semantic event.

## Composition

```scala
val codec: EventCodec[Foo | Bar] =
  EventCodec.union(FooCodec, BarCodec) {
    case foo: Foo => Left(foo)
    case bar: Bar => Right(bar)
  }
```

The composed codec exposes Schema.Union(first, second). Internally it delegates
encoding and decoding to the selected child, preserving that child's leaf
identity and version. Union routing uses leaf identities regardless of nesting
or order. Duplicate identities are rejected.

A composed codec has no public bytes-only encode/decode API: runtime decoding
requires the stored schema identity as well as the payload version and bytes.

## Journal migration

Journal format 2 persists namespace and name separately, followed by payload
version and bytes. It also reads format 1 records by mapping the old tag to
Schema.Simple("", oldTag). To replay an existing journal, the codec must support
that legacy identity. Moving to a nonempty namespace requires explicit
migration support. The runtime registration schema retains its legacy identity.

This is a source-breaking API change. EventCodec no longer extends Codec,
client implementations extend EventCodec.Simple, and JournalEntry uses schema
instead of tag. Old binaries cannot read new format 2 records.
