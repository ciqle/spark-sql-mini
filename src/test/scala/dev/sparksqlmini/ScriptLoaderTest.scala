package dev.sparksqlmini

import java.nio.file.{Files, Path}

import dev.sparksqlmini.Statement.SparkSql
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ScriptLoaderTest {
  @Test
  def rejectsANonFileIncludeAtItsSource(@TempDir directory: Path): Unit = {
    val entry = Files.writeString(directory.resolve("entry.sql"), "SELECT 1; INCLUDE 'folder';")
    val folder = Files.createDirectory(directory.resolve("folder"))

    val error = assertThrows(classOf[IllegalArgumentException], () => ScriptLoader.load(entry))
    assertTrue(error.getMessage.contains(s"${entry.toRealPath()} (statement 2)"))
    assertTrue(error.getMessage.contains(s"script '${folder.toRealPath()}' must be a regular file"))
  }

  @Test
  def expandsDeepIncludeChainsWithoutUsingTheCallStack(@TempDir directory: Path): Unit = {
    val depth = 2048
    for (index <- 0 until depth) {
      Files.writeString(directory.resolve(s"$index.sql"), s"INCLUDE '${index + 1}.sql';")
    }
    val leaf = Files.writeString(directory.resolve(s"$depth.sql"), "SELECT 1;")

    assertEquals(
      Vector(ScriptLoader.Entry(leaf.toRealPath(), 1, SparkSql("SELECT 1"))),
      ScriptLoader.load(directory.resolve("0.sql"))
    )
  }
}
