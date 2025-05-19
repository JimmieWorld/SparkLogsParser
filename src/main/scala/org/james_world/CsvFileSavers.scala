package org.james_world

import java.io.{BufferedWriter, File, FileWriter}

object CsvFileSavers {
    /**
     * Универсальный метод для сохранения результатов в CSV.
     *
     * @param data Данные для сохранения.
     * @param outputPath Путь для сохранения CSV-файла.
     * @param header Названия колонок.
     * @param delimiter Разделитель (по умолчанию ",").
     */
    def saveResultsToCSV[A](
        data: Seq[A],
        outputPath: String,
        header: Option[Seq[String]] = None,
        delimiter: String = ","
    )(decompose: A => Seq[Any]): Unit = {
        val file = new File(outputPath)
        val writer = new BufferedWriter(new FileWriter(file))

        try {
            header.foreach { cols =>
                writer.write(cols.mkString(delimiter))
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