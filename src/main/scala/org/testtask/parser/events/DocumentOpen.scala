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

  override def keys(): Array[String] = Array("DOC_OPEN")

  override def parse(
      context: ParsingContext
  ): Unit = {
    val lineWithDoc = context.lines.next()
    val splitLineWithDoc = lineWithDoc.split("\\s+")

    val searchId = splitLineWithDoc(splitLineWithDoc.length - 2)
    val documentId = splitLineWithDoc.last

    val timestamp = DateTimeParser.parseDateTime(splitLineWithDoc(1), context)

    context.sessionBuilder.docOpens :+= DocumentOpen(timestamp, searchId, documentId)
  }
}
