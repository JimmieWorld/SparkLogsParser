package org.james_world

import java.io.{BufferedWriter, File, FileWriter}

class FileSaver {
    /**
     * Универсальный метод для сохранения результатов в CSV.
     *
     * @param results Результаты анализа (ключ -> значение).
     * @param outputPath Путь для сохранения CSV-файла.
     */
    def saveResultsToCSV(results: Map[_, _], outputPath: String): Unit = {
        val file = new File(outputPath)
        val writer = new BufferedWriter(new FileWriter(file))

        try {
            results.foreach { case (key, value) =>
                val keyColumns = decomposeKey(key)
                writer.write(s"${keyColumns.mkString(",")},$value\n")
            }
        } finally {
            writer.close()
        }
    }

    /**
     * Метод для разбиения ключа на составные части.
     *
     * @param key Ключ (может быть простым значением или составным, например, кортежем).
     * @return Список строковых представлений частей ключа.
     */
    private def decomposeKey(key: Any): Seq[String] = key match {
        case tuple: Product => tuple.productIterator.map(_.toString).toSeq
        case single => Seq(single.toString)
    }
}