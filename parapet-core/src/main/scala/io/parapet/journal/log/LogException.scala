package io.parapet.journal.log

import java.io.IOException
import java.nio.file.Path

class CorruptLogException(message: String, cause: Throwable = null) extends IOException(message, cause)
final class InvalidChecksumException(message: String)               extends CorruptLogException(message)
final class UnsupportedLogVersionException(message: String)         extends IOException(message)

/** Thrown by init in RecoveryMode.Fail when the previous run did not stop cleanly, so entries may have been lost before
  * they reached the log. `path` is the open file the run left or, when there is none, the running marker. No file is
  * changed.
  */
final class UncleanStopException(val path: Path)
    extends IOException(
      s"the previous run did not stop cleanly: found $path. Restart in RecoveryMode.Repair, which seals or discards " +
        "an open file; copy that file first to keep the bytes that Repair drops."
    )
