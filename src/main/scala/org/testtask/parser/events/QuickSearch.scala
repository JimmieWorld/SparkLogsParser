package org.testtask.parser.events

import org.testtask.parser.events.utils.{DateTimeParser, SearchResultParser}
import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

case class QuickSearch(
    timestamp: Option[LocalDateTime],
    queryText: String,
    searchResult: SearchResult
) extends Event

object QuickSearch extends EventParser {
  override def parse(
      context: ParsingContext
  ): Unit = {
    val fileName = context.fileName
    val bufferedIt = context.lines
    val errorStatsAcc = context.errorStatsAcc

    val firstLine = bufferedIt.next()
    val splitFirstLine = splitQuickSearchLine(firstLine)

    val timestamp = DateTimeParser.parseTimestamp(splitFirstLine(1), errorStatsAcc, fileName)
    val queryText = splitFirstLine.last.stripPrefix("{").stripSuffix("}")

    val searchResult = SearchResult.parse(context)

    context.sessionBuilder.quickSearches :+= QuickSearch(timestamp, queryText, searchResult)
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

    val timestampPart = restAfterType.substring(0, queryStartIndex).trim
    val queryPart = restAfterType.substring(queryStartIndex).trim

    Array(eventType, timestampPart, queryPart)
  }
}
