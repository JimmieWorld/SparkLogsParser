package org.testTask.parser.events

import org.testTask.parser.events.utils.DateTimeParser
import org.testTask.parser.processors.{ParsingContext, SessionBuilder}

import java.time.LocalDateTime

case class DocumentOpen(
    timestamp: Option[LocalDateTime],
    searchId: String,
    documentId: String
) extends Event

object DocumentOpen extends EventParser {
  override def parse(
      context: ParsingContext
  ): Event = {
    val fileName = context.fileName
    val bufferedIt = context.lines
    val errorStatsAcc = context.errorStatsAcc

    val lineWithDoc = bufferedIt.next()
    val splitLineWithDoc = lineWithDoc.split("\\s+")

    val searchId = splitLineWithDoc(splitLineWithDoc.length - 2)
    val documentId = splitLineWithDoc.last

    val rawTimestamp = DateTimeParser.parseTimestamp(splitLineWithDoc(1), errorStatsAcc, fileName)
    val timestamp = if (rawTimestamp.isEmpty) {
      context.searchTimestamps.get(searchId)
    } else {
      rawTimestamp
    }

    DocumentOpen(
      timestamp = timestamp,
      searchId = searchId,
      documentId = documentId
    )
  }
}
