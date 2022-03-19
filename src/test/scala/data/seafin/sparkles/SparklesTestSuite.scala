package data.seafin.sparkles

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Directory

import org.scalatest.funsuite.AnyFunSuite

class SparklesTestSuite extends AnyFunSuite {

  /**
    * Creates a temp dir, which is then passed to `f` and will be deleted after `f` returns
    */
  protected def withTempDir(f: File => Unit, namePrefix: String = "sparkles"): Unit = {
    val tempDir = Files.createTempDirectory(namePrefix).toFile
    try f(tempDir)
    finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(dir: File): Unit = {
    val directory = new Directory(dir)
    directory.deleteRecursively()
  }
}
