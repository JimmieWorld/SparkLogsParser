package org.testtask

import java.io.{BufferedWriter, File, FileWriter}

object Utils {

  /** Универсальный метод для сохранения результатов в CSV.
    *
    * @param data Данные для сохранения.
    * @param outputPath Путь для сохранения CSV-файла.
    * @param header Названия колонок.
    * @param delimiter Разделитель (по умолчанию ",").
    * @param decompose Функция, которая преобразует элемент типа A
    *                  в набор значений, подходящих для записи в CSV
    */
  def saveToCSV[A](
      data: Array[A],
      outputPath: String,
      header: Array[String] = Array.empty,
      delimiter: String = ","
  )(decompose: A => Array[Any]): Unit = {
    val file = new File(outputPath)
    val writer = new BufferedWriter(new FileWriter(file))

    try {
      if (header.nonEmpty) {
        writer.write(header.mkString(delimiter))
        writer.newLine()
      }

      data.foreach { item =>
        val columns = decompose(item).map(_.toString)
        writer.write(columns.mkString(delimiter))
        writer.newLine()
      }

    } finally {
      writer.close()
    }
  }
}
