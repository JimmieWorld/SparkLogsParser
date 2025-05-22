package org.james_world.Events

import org.james_world.ErrorStatsAccumulator
import org.james_world.Extractors.EventAttributesExtractors.SearchResultExtractor
import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}

import scala.collection.BufferedIterator
import java.time.LocalDateTime

case class QuickSearchEvent(
    timestamp: Option[LocalDateTime],
    searchId: String,
    queryText: String,
    relatedDocuments: Seq[String]
) extends Event

object QuickSearchEvent extends EventParser {
    override def parse(
        bufferedIt: BufferedIterator[String],
        errorStatsAcc: ErrorStatsAccumulator
    ): Option[Event] = {
        if (!bufferedIt.hasNext) return None

        val firstLine = bufferedIt.next()
        val splitFirstLine = splitQuickSearchLine(firstLine)
        val eventName = "QS"
        if (!firstLine.startsWith(eventName)) {
            errorStatsAcc.add(("QuickSearchInvalidFormat", s"Expected QS line, got: ${firstLine.take(50)}..."))
            return None
        }

        if (splitFirstLine.length < 2) {
            errorStatsAcc.add((
                "QuickSearchMissingData",
                s"[$eventName] Not enough tokens in line: $firstLine"
            ))
            return None
        }

        val timestamp = DateTimeExtractor.extractTimestamp(splitFirstLine(1), eventName, errorStatsAcc)
        val queryText = splitFirstLine.last.stripPrefix("{").stripSuffix("}")


        if (!bufferedIt.hasNext || bufferedIt.head.trim.charAt(0).isUpper) {
            errorStatsAcc.add((
                "QuickSearchMissingSearchResult",
                s"[$eventName] Expected search result line after: $firstLine"
            ))
            return Some(QuickSearchEvent(
                timestamp = timestamp,
                searchId = "",
                queryText = queryText,
                relatedDocuments = Seq.empty
            ))
        }

        val (searchId, relatedDocuments) = SearchResultExtractor.extractSearchResult(bufferedIt, errorStatsAcc)

        Some(QuickSearchEvent(
            timestamp = timestamp,
            searchId = searchId,
            queryText = queryText,
            relatedDocuments = relatedDocuments
        ))
    }

    def splitQuickSearchLine(line: String): Array[String] = {
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