package org.james_world

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.james_world.ErrorStatsAccumulatorDef.ErrorStats
import org.james_world.Parsers.TextFileParser

import java.io.File
import java.time.LocalDate
import scala.io.Source
import scala.util.Using

object Main {

    def main(args: Array[String]): Unit = {
        val sc = initSparkContext()
        val errorStatsAcc = new ErrorStatsAccumulator()
        sc.register(errorStatsAcc)

        try {
            val sessionsRDD = processFiles(sc, filePath = "src/main/resources/data", errorStatsAcc)

            val res1 = Tasks.task1(sessionsRDD, "ACC_45616")
            val res2 = Tasks.task2(sessionsRDD)

            println(s"Task1: $res1")
            CsvFileSavers.saveResultsToCSV(
                res2,
                "src/main/resources/results/task2_results.csv"
            )(tuple => Seq(tuple._1, tuple._2, tuple._3))

            saveErrorStatistics(errorStatsAcc.value, "src/main/resources/results/errors.csv")

        } catch {
            case e: Exception =>
                println(s"[ERROR] Failed to process files: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            sc.stop()
        }
    }

    private def initSparkContext(): SparkContext = {
        val conf = new SparkConf()
            .setAppName("SessionProcessor")
            .setMaster("local[*]")
            .set("spark.ui.enabled", "false")

        new SparkContext(conf)
    }

    private def processFiles(
        sc: SparkContext,
        filePath: String,
        errorStatsAcc: ErrorStatsAccumulator
    ): RDD[Session] = {
        val dir = new File(filePath)
        if (!dir.exists() || !dir.isDirectory) {
            throw new IllegalArgumentException(s"Invalid directory path: $filePath")
        }

        val files = dir.listFiles().toSeq
        val filesRDD = sc.parallelize(files)

        filesRDD.map { file =>
            val lines = Using(Source.fromFile(file, "Windows-1251")) { source =>
                source.getLines().toSeq
            }.getOrElse {
                errorStatsAcc.add(("FileReadFailed", s"Failed to read file: ${file.getAbsolutePath}"))
                Seq.empty
            }

            TextFileParser.processLines(lines, fileName = file.getName, errorStatsAcc)
        }
    }

    private def saveErrorStatistics(
        errorStats: ErrorStats,
        outputPath: String
    ): Unit = {

        val errorSeq = errorStats.toSeq.map { case (errorType, (count, samples)) =>
            (errorType, count.toString, samples.mkString("; "))
        }

        CsvFileSavers.saveResultsToCSV(
            data = errorSeq,
            outputPath = outputPath,
            header = Some(Seq("ErrorType", "Count", "Samples"))
        ) { case (errorType, countStr, samplesStr) =>
            Seq(errorType, countStr, samplesStr)
        }
    }

    private object Tasks {
        /**
         * Подсчёт количества раз, когда искали определенный документ в карточке поиска.
         *
         * @param documentId Идентификатор документа (например, "ACC_45616").
         * @param sessions список объектов сессий из лога
         * @return Количество раз, когда искали указанный документ.
         */
        def task1(sessions: RDD[Session], documentId: String): Int = {
            sessions
                .flatMap { session =>
                    session.cardSearches.filter { cs =>
                        cs.relatedDocuments.contains(documentId)
                    }
                }
                .count()
                .toInt
        }

        /**
         * Подсчёт количества открытий документов, найденных через быстрый поиск, за каждый день.
         *
         * @param sessions список объектов сессий из лога
         * @return Список элементов типа (date, documentId, count).
         */
        def task2(sessions: RDD[Session]): Seq[(LocalDate, String, Int)] = {
            sessions
                .flatMap { session =>
                    val quickSearchIds = session.quickSearches.map(_.searchId).toSet

                    session.docOpens.flatMap { docOpen =>
                        docOpen.timestamp.map { ts =>
                            (ts.toLocalDate, docOpen.documentId)
                        }.filter(_ => quickSearchIds.contains(docOpen.searchId))
                    }
                }
                .map(dateDoc => (dateDoc, 1))
                .reduceByKey(_ + _)
                .map { case ((date, docId), count) => (date, docId, count) }
                .collect()
                .toSeq
        }
    }


}
