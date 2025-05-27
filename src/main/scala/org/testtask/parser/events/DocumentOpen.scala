package org.testtask.parser.events

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.processors.{ParsingContext, SessionBuilder}

import java.time.LocalDateTime

case class DocumentOpen(
    var timestamp: Option[LocalDateTime],
    searchId: String,
    documentId: String
) extends Event

object DocumentOpen extends EventParser {
  override def parse(
      context: ParsingContext
  ): Unit = {
    val fileName = context.fileName
    val bufferedIt = context.lines
    val errorStatsAcc = context.errorStatsAcc

    val lineWithDoc = bufferedIt.next()
    val splitLineWithDoc = lineWithDoc.split("\\s+")

    val searchId = splitLineWithDoc(splitLineWithDoc.length - 2)
    val documentId = splitLineWithDoc.last

    val timestamp = DateTimeParser.parseTimestamp(splitLineWithDoc(1), errorStatsAcc, fileName)

    context.sessionBuilder.docOpens :+= DocumentOpen(timestamp, searchId, documentId)
  }
}
