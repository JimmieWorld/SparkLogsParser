package org.james_world

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.james_world.Events.{CardSearchEvent, DocumentOpenEvent, QuickSearchEvent}
import java.io.File
import java.time.LocalDate
import scala.io.Source
import scala.util.Using
import org.james_world.CsvFileSavers

object Main {

    /**
     * Метод для подсчета количества раз, когда искали определенный документ в карточке поиска.
     *
     * @param documentId Идентификатор документа (например, "ACC_45616").
     * @param sessions список объектов сессий из лога
     * @return Количество раз, когда искали указанный документ.
     */
    def countSearchesForDocument(sessions: RDD[Session], documentId: String): Long = {
        sessions.flatMap { session =>
            session.events.collect {
                case cs: CardSearchEvent if cs.queriesTexts.contains(documentId) => 1L
            }
        }.fold(0)(_ + _)
    }

    /**
     * Метод для подсчета количества открытий документов, найденных через быстрый поиск, за каждый день.
     *
     * @param sessions список объектов сессий из лога
     * @return Список элементов типа (date, documentId, count).
     */
    def countDocumentOpens(sessions: RDD[Session]): Seq[(LocalDate, String, Int)] = {
        sessions.flatMap { session =>
            val quickSearches = session.events.collect {
                case qs: QuickSearchEvent => (qs.timestamp.toLocalDate, qs.searchId, qs.relatedDocuments)
            }

            val docsOpen = session.events.collect {
                case docOpen: DocumentOpenEvent =>
                    val isQuickSearchDocOpen = quickSearches.exists { case (_,search,_) =>
                        search == docOpen.searchId
                    }
                    if (isQuickSearchDocOpen) Some((docOpen.timestamp.toLocalDate, docOpen.documentId)) else None
                case _ => None
            }.flatten

            docsOpen.map(dateDoc => dateDoc -> 1)
        }
        .reduceByKey(_ + _)
        .map { case ((date, docId), count) => (date, docId, count) }
        .collect()
        .toSeq
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ConsultantPlusLogs").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val filePath = "src/main/resources/data"
        val files = new File(filePath).listFiles()
        val filesRDD = sc.parallelize(files)

        val sessionsRDD: RDD[Session] = filesRDD.map { file =>
            val lines = Using(Source.fromFile(file, "Windows-1251")) { source =>
                source.getLines().toSeq
            }.getOrElse {
                Seq.empty
            }
            Session.processLines(lines, fileName = file.getName)
        }

        val res1 = countSearchesForDocument(sessionsRDD, "ACC_45616")
        val res2 = countDocumentOpens(sessionsRDD)

        println(s"Task1: $res1")
        CsvFileSavers.saveResultsToCSV(
            res2,
            "src\\main\\resources\\results\\open_results.csv"
        ) (tuple => Seq(tuple._1, tuple._2, tuple._3))

        sc.stop()
    }
}