package dev.sparksqlmini

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, InvalidPathException, Path}
import scala.collection.mutable

import dev.sparksqlmini.Statement.Include

/** Expands file dependencies before the resulting statements are executed. */
private[sparksqlmini] object ScriptLoader {
  final case class Entry(file: Path, number: Int, statement: Statement)
  private final case class Frame(file: Path, statements: Iterator[(Statement, Int)])

  def load(path: Path): Vector[Entry] = {
    val entries = Vector.newBuilder[Entry]
    val completed = mutable.HashSet.empty[Path]
    val active = mutable.HashSet.empty[Path]
    val stack = mutable.ArrayDeque.empty[Frame]

    def push(requested: Path, location: String): Unit = {
      val file = try requested.toRealPath()
      catch {
        case error: IOException =>
          throw new IllegalArgumentException(s"$location: cannot open script '$requested': ${error.getMessage}", error)
      }
      if (active.contains(file)) {
        val chain = (stack.iterator.map(_.file) ++ Iterator.single(file)).mkString(" -> ")
        throw new IllegalArgumentException(s"$location: INCLUDE cycle: $chain")
      }
      if (!completed.contains(file)) {
        val statements = try StatementParser.parse(Files.readString(file, UTF_8))
        catch {
          case error: IOException =>
            throw new IllegalArgumentException(s"$location: cannot read script '$file': ${error.getMessage}", error)
          case error: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$location: invalid script '$file': ${error.getMessage}", error)
        }
        active += file
        stack.append(Frame(file, statements.iterator.zipWithIndex))
      }
    }

    push(path, "Entry script")
    while (stack.nonEmpty) {
      val Frame(file, statements) = stack.last
      if (statements.hasNext) {
        statements.next() match {
          case (Include(includePath), index) =>
            val origin = s"$file (statement ${index + 1})"
            val relative = try file.getFileSystem.getPath(includePath)
            catch {
              case error: InvalidPathException =>
                throw new IllegalArgumentException(s"$origin: invalid INCLUDE path '$includePath'", error)
            }
            require(!relative.isAbsolute, s"$origin: INCLUDE requires a relative path: '$includePath'")
            push(file.resolveSibling(relative), origin)
          case (statement, index) => entries += Entry(file, index + 1, statement)
        }
      } else {
        stack.removeLast()
        active -= file
        completed += file
      }
    }

    entries.result()
  }
}
