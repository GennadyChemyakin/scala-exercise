package hw1

import java.io.{File, FileWriter}

trait Logger {
  def log(msg: String)
}

class ConsoleLogger extends Logger {
  override def log(msg: String): Unit = println(msg)
}

class FileLogger extends Logger {
  override def log(msg: String): Unit = {
    val path = "/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/log"

    withFileWriter(new FileWriter(new File(path), true)) { writer =>
      writer.append(msg)
    }

  }

  def withFileWriter(writer: FileWriter)(block: FileWriter => Unit): Unit = {
    try {
      block(writer)
    } finally {
      writer.close()
    }
  }
}

trait Auth {
  this: Logger =>

  def login(id: String, password: String): Boolean
}
