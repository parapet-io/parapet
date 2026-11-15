# Codecs for events and values

The runtime uses `Codec[A]`. A codec owns its byte representation, including any
discriminator, schema, or version information it chooses to encode. Parapet does
not add application schema metadata or dispatch historical payload versions.

## Derived codecs

```scala
import io.parapet.{Codec, Event}

case class Ping(message: String) extends Event derives Codec
```

`CodecAvailability[A]` discovers a `Codec[A]` from implicit scope. It also works
for non-event types such as `String`. Ordinary processes can omit their input
codec; replayable processes require one when journaling is enabled.

Derivation encodes product fields in declaration order using their codecs. It
does not add schema metadata or automatically migrate old layouts.

## Custom representations and versioning

```scala
import io.parapet.{Codec, Event}
import java.nio.ByteBuffer
import scala.util.Try

case class Add(amount: Long) extends Event

given Codec[Add] with
  def encode(value: Add): Try[Array[Byte]] = Try {
    ByteBuffer.allocate(12).putInt(2).putLong(value.amount).array()
  }

  def decode(buffer: ByteBuffer): Try[Add] = Try {
    buffer.getInt() match
      case 1 => Add(buffer.getInt().toLong)
      case 2 => Add(buffer.getLong())
      case version =>
        throw new IllegalArgumentException(s"Unsupported Add version: $version")
  }
```

The buffer decoder consumes one value and advances the buffer position. The
provided `decode(Array[Byte])` adapter also rejects trailing bytes. A union
codec can encode and dispatch its own discriminator in the same way.

## Journal format change

Journal entries now hold opaque application bytes and a runtime record kind
(`Event` or `Registration`). Registration routing does not depend on application
schemas or payload contents.

The delivery segment codec version is now 2; standalone journal entry blobs use
format 3. Earlier schema-bearing journal formats are rejected and require an
explicit migration. Removing the old metadata without migrating application
payloads would lose the information their decoders previously depended on.
