package org.james_world.events

import org.james_world.ParsingContext
import org.james_world.events.utils.DateTimeParser

import java.time.LocalDateTime

case class DocumentOpen(
    timestamp: Option[LocalDateTime],
    searchId: String,
    documentId: String
) extends Event

object DocumentOpen extends EventParser {
  private var searchTimestamps: Map[String, LocalDateTime] = Map.empty

  def clearSearchTimestamps(): Unit = {
    searchTimestamps = Map.empty
  }
  override def parse(
      context: ParsingContext
  ): Event = {
    val lineWithDoc = context.bufferedIt.next()
    val splitLineWithDoc = lineWithDoc.split("\\s+")

    val searchId = splitLineWithDoc(splitLineWithDoc.length - 2)
    val documentId = splitLineWithDoc.last

    val rawTimestamp = DateTimeParser.parseTimestamp(splitLineWithDoc(1), context.errorStatsAcc)
    val timestamp = if (rawTimestamp.isEmpty) {
      searchTimestamps.get(searchId)
    } else {
      rawTimestamp
    }

    DocumentOpen(
      timestamp = timestamp,
      searchId = searchId,
      documentId = documentId
    )
  }

  def addSearchTimestamp(timestamp: Map[String, Option[LocalDateTime]]): Unit = {
    val validTimestamps = timestamp.collect { case (id, Some(dt)) =>
      id -> dt
    }
    searchTimestamps ++= validTimestamps
  }
}
