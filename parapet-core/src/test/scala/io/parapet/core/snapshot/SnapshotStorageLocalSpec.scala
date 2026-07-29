package io.parapet.core.snapshot

import io.parapet.core.TestUtils.{*, given}
import io.parapet.core.snapshot.SnapshotStorageLocal.DirKeyBytes
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.HexFormat

class SnapshotStorageLocalSpec extends AnyFunSuite:

  private val ref = ProcessRef[Event]("storage-spec/process a") // ref with fs-hostile chars

  private def snapshot(id: Long, parentId: Long = 0L, seq: Long = 0L, createdAt: Long = 0L, state: String = "state") =
    Snapshot(
      Snapshot.Metadata(ref, id = id, parentId = parentId, seq = seq, createdAt = createdAt, schemaVersion = 1L),
      state.getBytes(UTF_8)
    )

  private def newStorage(dir: Path = Files.createTempDirectory("storage-spec")) =
    (dir, new SnapshotStorageLocal[TestIO](SnapshotStorageLocal.Config(dir)))

  private def refDir(dataDir: Path): Path =
    val digest =
      MessageDigest
        .getInstance("SHA-256")
        .digest(ref.value.getBytes(UTF_8))
    dataDir.resolve(HexFormat.of().formatHex(digest, 0, DirKeyBytes))

  test("store/read round-trip preserves metadata and data") {
    val (_, storage) = newStorage()
    val original     = snapshot(id = 1L, parentId = 0L, seq = 42L, createdAt = 1234L, state = "hello")
    storage.store(original).unsafeRun()

    val loaded = storage.read(ref, 1L).unsafeRun().getOrElse(fail("missing"))
    loaded.metadata shouldBe original.metadata
    new String(loaded.data, UTF_8) shouldBe "hello"
  }

  test("read of an absent id returns None") {
    val (_, storage) = newStorage()
    storage.read(ref, 99L).unsafeRun() shouldBe None
  }

  test("storing the same (ref, id) twice fails") {
    val (_, storage) = newStorage()
    storage.store(snapshot(id = 1L)).unsafeRun()
    an[IllegalStateException] should be thrownBy storage.store(snapshot(id = 1L)).unsafeRun()
  }

  test("latest returns the highest id; latestBefore filters by createdAt inclusively") {
    val (_, storage) = newStorage()
    storage.store(snapshot(id = 1L, createdAt = 100L)).unsafeRun()
    storage.store(snapshot(id = 2L, createdAt = 200L)).unsafeRun()
    storage.store(snapshot(id = 3L, createdAt = 300L)).unsafeRun()

    storage.latest(ref).unsafeRun().map(_.metadata.id) shouldBe Some(3L)
    storage.latestBefore(ref, 250L).unsafeRun().map(_.metadata.id) shouldBe Some(2L)
    storage.latestBefore(ref, 200L).unsafeRun().map(_.metadata.id) shouldBe Some(2L) // inclusive
    storage.latestBefore(ref, 99L).unsafeRun() shouldBe None
  }

  test("a new storage instance over the same directory sees existing snapshots") {
    val (dir, first) = newStorage()
    first.store(snapshot(id = 7L, state = "persisted")).unsafeRun()

    val (_, second) = newStorage(dir)
    second.latest(ref).unsafeRun().map(_.metadata.id) shouldBe Some(7L)
  }

  test("read fails on a corrupt entry; latest skips it and returns the latest intact one") {
    val (dir, storage) = newStorage()
    storage.store(snapshot(id = 1L, state = "good")).unsafeRun()
    storage.store(snapshot(id = 2L, state = "to-corrupt")).unsafeRun()

    // flip a byte in the data section of snapshot 2
    val f2    = refDir(dir).resolve("00000000000000000002.snap")
    val bytes = Files.readAllBytes(f2)
    bytes(bytes.length - 10) = (bytes(bytes.length - 10) ^ 0xff).toByte
    Files.write(f2, bytes)

    a[SnapshotBinaryFormat.CorruptSnapshotException] should be thrownBy storage.read(ref, 2L).unsafeRun()
    storage.latest(ref).unsafeRun().map(_.metadata.id) shouldBe Some(1L)
  }

  test("a foreign file is rejected by magic, not decoded into garbage") {
    val (dir, storage) = newStorage()
    storage.store(snapshot(id = 1L)).unsafeRun()
    Files.write(refDir(dir).resolve("00000000000000000002.snap"), "not a snapshot at all".getBytes(UTF_8))

    val error = the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy storage.read(ref, 2L).unsafeRun()
    error.getMessage should include("magic")
    storage.latest(ref).unsafeRun().map(_.metadata.id) shouldBe Some(1L)
  }
