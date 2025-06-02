package org.testtask.parser.events

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.processors.{ParsingContext, SessionBuilder}

import java.time.LocalDateTime

case class DocumentOpen(
    var dateTime: Option[LocalDateTime],
    searchId: String,
    documentId: String
) extends Event

object DocumentOpen extends EventParser {

  override def keys(): Array[String] = Array("DOC_OPEN")

  override def parse(
      context: ParsingContext
  ): Unit = {
    val lineWithDoc = context.lines.next()
    val Array(_, rawDateTime, searchId, documentId) = lineWithDoc.split("\\s+")

    val dateTime = DateTimeParser.parseDateTime(rawDateTime, context)

    context.sessionBuilder.docOpens :+= DocumentOpen(dateTime, searchId, documentId)
  }
}
