package org.testtask

import java.io.{BufferedWriter, File, FileWriter}

object Utils {

  def saveToCSV[A](
      items: Array[A],
      outputPath: String,
      header: Array[String] = Array.empty,
      columnSeparator: String = ","
  )(convertItemToColumns: A => Array[Any]): Unit = {
    val file = new File(outputPath)
    val writer = new BufferedWriter(new FileWriter(file))

    try {
      if (header.nonEmpty) {
        writer.write(header.mkString(columnSeparator))
        writer.newLine()
      }

      items.foreach { item =>
        val columns = convertItemToColumns(item).map(_.toString)
        writer.write(columns.mkString(columnSeparator))
        writer.newLine()
      }

    } finally {
      writer.close()
    }
  }
}
