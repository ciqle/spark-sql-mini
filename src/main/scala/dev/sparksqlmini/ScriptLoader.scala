package dev.sparksqlmini

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, InvalidPathException, Path}
import scala.collection.mutable

import dev.sparksqlmini.Statement.Include

/** Expands file dependencies before the resulting statements are executed. */
private[sparksqlmini] object ScriptLoader {
  final case class Entry(file: Path, number: Int, statement: Statement)

  def load(path: Path): Vector[Entry] = {
    val entries = Vector.newBuilder[Entry]
    val visited = mutable.HashSet.empty[Path]

    def visit(requested: Path, stack: Vector[Path], location: String): Unit = {
      val file = try requested.toRealPath()
      catch {
        case error: IOException =>
          throw new IllegalArgumentException(s"$location: cannot open script '$requested': ${error.getMessage}", error)
      }
      if (stack.contains(file)) {
        val chain = (stack :+ file).mkString(" -> ")
        throw new IllegalArgumentException(s"$location: INCLUDE cycle: $chain")
      }
      if (visited.add(file)) {
        val statements = try StatementParser.parse(Files.readString(file, UTF_8))
        catch {
          case error: IOException =>
            throw new IllegalArgumentException(s"$location: cannot read script '$file': ${error.getMessage}", error)
          case error: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$location: invalid script '$file': ${error.getMessage}", error)
        }
        val nextStack = stack :+ file
        statements.zipWithIndex.foreach {
          case (Include(includePath), index) =>
            val origin = s"$file (statement ${index + 1})"
            val relative = try file.getFileSystem.getPath(includePath)
            catch {
              case error: InvalidPathException =>
                throw new IllegalArgumentException(s"$origin: invalid INCLUDE path '$includePath'", error)
            }
            require(!relative.isAbsolute, s"$origin: INCLUDE requires a relative path: '$includePath'")
            visit(file.resolveSibling(relative), nextStack, origin)
          case (statement, index) => entries += Entry(file, index + 1, statement)
        }
      }
    }

    visit(path, Vector.empty, "Entry script")
    entries.result()
  }
}
