package org.testtask.tasks

import org.apache.spark.rdd.RDD
import org.testtask.Utils
import org.testtask.parser.Session

object Task2 {
  def execute(sessions: RDD[Session]): Unit = {
    val result = sessions
      .flatMap { session =>
        session.quickSearches.flatMap { quickSearch =>
          quickSearch.searchResult.docOpens.map { docOpen =>
            ((docOpen.timestamp.get.toLocalDate, docOpen.documentId), 1)
          }
        }
      }
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId, count) }
      .collect()

    Utils.saveToCSV(
      result,
      "src/main/resources/results/task2_result.csv"
    )(_.productIterator.map(_.toString).toArray)
  }
}
