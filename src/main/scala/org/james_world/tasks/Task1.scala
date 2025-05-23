package org.james_world.tasks

import org.apache.spark.rdd.RDD
import org.james_world.Session

case class Task1(documentId: String, searchCount: Int)

object Task1 {
  def execute(sessions: RDD[Session], documentId: String): Task1 = {
    val searchCount = sessions
      .flatMap { session =>
        session.cardSearches.filter { cs =>
          cs.relatedDocuments.contains(documentId)
        }
      }
      .count()
      .toInt

    Task1(
      documentId = documentId,
      searchCount = searchCount
    )
  }
}
