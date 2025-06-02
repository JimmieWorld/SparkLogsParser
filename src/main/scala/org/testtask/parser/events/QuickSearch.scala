package org.testtask.parser.events

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

case class QuickSearch(
    dateTime: Option[LocalDateTime],
    queryText: String,
    searchResult: SearchResult
) extends Event

object QuickSearch extends EventParser {

  override def keys(): Array[String] = Array("QS")

  override def parse(
      context: ParsingContext
  ): Unit = {
    val firstLine = context.lines.next()
    val Array(_, rawDateTime, rawQueryText) = firstLine.split("\\s+")

    val dateTime = DateTimeParser.parseDateTime(rawDateTime, context)
    val queryText = rawQueryText.stripPrefix("{").stripSuffix("}")

    val searchResult = SearchResult.parse(context)

    context.sessionBuilder.quickSearches :+= QuickSearch(dateTime, queryText, searchResult)
  }
}
