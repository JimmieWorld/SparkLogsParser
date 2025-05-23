package org.james_world.events

import org.james_world.ParsingContext
import org.james_world.events.utils.{DateTimeParser, SearchResultParser}

import java.time.LocalDateTime

case class QuickSearch(
    timestamp: Option[LocalDateTime],
    searchId: String,
    queryText: String,
    relatedDocuments: Seq[String],
    docOpens: Seq[DocumentOpen]
) extends Event

object QuickSearch extends EventParser {
  override def parse(
      context: ParsingContext
  ): Event = {
    val firstLine = context.bufferedIt.next()
    val splitFirstLine = splitQuickSearchLine(firstLine)

    val timestamp = DateTimeParser.parseTimestamp(splitFirstLine(1), context.errorStatsAcc)
    val queryText = splitFirstLine.last.stripPrefix("{").stripSuffix("}")

    val (searchId, relatedDocuments) = SearchResultParser.parserSearchResult(context)

    QuickSearch(
      timestamp = timestamp,
      searchId = searchId,
      queryText = queryText,
      relatedDocuments = relatedDocuments,
      docOpens = Nil
    )
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
