package io.parapet.journal

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.Locale

object FileUtils {

  private val isWindows =
    System
      .getProperty("os.name")
      .toLowerCase(Locale.ROOT)
      .startsWith("windows")

  /** Forces a directory's entries to storage, so file creations, renames and deletions in it survive a host crash. Does
    * nothing on Windows, where Java cannot open a directory to force it.
    */
  def forceDirectory(path: Path): Unit =
    if !isWindows then
      val channel =
        FileChannel.open(path, StandardOpenOption.READ)

      try
        channel.force(true)
      finally
        channel.close()

  /** Opens the file for writing because Windows only flushes a file through a writable handle. */
  def forceFile(path: Path): Unit =
    val channel = FileChannel.open(path, StandardOpenOption.WRITE)
    try channel.force(true)
    finally channel.close()

  def forceDirectoryChain(path: Path): Unit =
    // Existing directories may be remnants of an attempt that failed or crashed before its parent entries were
    // forced. Re-establish the full chain on the first access after startup or invalidation
    var current = path.toAbsolutePath.normalize()
    while current != null do
      forceDirectory(current)
      current = current.getParent

  def writeFully(channel: WritableByteChannel, buffer: ByteBuffer): Unit =
    while buffer.hasRemaining do
      val written = channel.write(buffer)
      if (written <= 0) throw new java.io.IOException("writing made no progress")

}
