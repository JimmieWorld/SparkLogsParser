package org.james_world.tasks

import org.apache.spark.rdd.RDD
import org.james_world.Session

import java.time.LocalDate

object Task2 {
  def execute(sessions: RDD[Session]): Array[(LocalDate, String, Int)] = {
    sessions
      .flatMap { session =>
        val quickSearchIds = session.quickSearches.map(_.searchId).toSet

        session.docOpens.flatMap { docOpen =>
          docOpen.timestamp
            .map { ts =>
              (ts.toLocalDate, docOpen.documentId)
            }
            .filter(_ => quickSearchIds.contains(docOpen.searchId))
        }
      }
      .map(dateDoc => (dateDoc, 1))
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId, count) }
      .collect()
  }
}
