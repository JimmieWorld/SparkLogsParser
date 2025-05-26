package org.testTask.tasks

import org.apache.spark.rdd.RDD
import org.testTask.parser.Session

object Task1 {

  val documentId: String = "ACC_45616"

  def execute(sessions: RDD[Session]): Unit = {
    val searchCount = sessions
      .flatMap { session =>
          session.cardSearches.filter(_.queriesTexts.exists(_.contains(documentId)))
      }
      .count()
      .toInt

    println(s"Task1 document: $documentId was found $searchCount times")
  }
}
