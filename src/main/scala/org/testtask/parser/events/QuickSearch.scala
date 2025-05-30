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
    val splitFirstLine = splitQuickSearchLine(firstLine)

    val dateTime = DateTimeParser.parseDateTime(splitFirstLine(1), context)
    val queryText = splitFirstLine.last.stripPrefix("{").stripSuffix("}")

    val searchResult = SearchResult.parse(context)

    context.sessionBuilder.quickSearches :+= QuickSearch(dateTime, queryText, searchResult)
  }

  private def splitQuickSearchLine(line: String): Array[String] = {
    val trimmed = line.trim

    val firstSpaceIndex = trimmed.indexOf(' ')
    if (firstSpaceIndex == -1) return Array(trimmed)

    val eventType = trimmed.substring(0, firstSpaceIndex)
    val restAfterType = trimmed.substring(firstSpaceIndex).trim

    val queryStartIndex = restAfterType.indexOf('{')

    if (queryStartIndex == -1) {
      return Array(eventType, restAfterType)
    }

    val dateTimePart = restAfterType.substring(0, queryStartIndex).trim
    val queryPart = restAfterType.substring(queryStartIndex).trim

    Array(eventType, dateTimePart, queryPart)
  }
}
