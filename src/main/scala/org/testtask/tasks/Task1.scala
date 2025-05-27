package org.testtask.tasks

import org.apache.spark.rdd.RDD
import org.testtask.parser.Session

object Task1 {

  val documentId: String = "ACC_45616"

  def execute(sessions: RDD[Session]): Unit = {
    val searchCount = sessions
      .map(_.cardSearches.count(_.queriesTexts.exists(_.contains(documentId))))
      .fold(0)(_+_)

    println(s"Task1 document: $documentId was found $searchCount times")
  }
}
