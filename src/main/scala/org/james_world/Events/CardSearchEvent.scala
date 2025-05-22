package org.james_world.Events

import org.james_world.ErrorStatsAccumulator
import org.james_world.Extractors.EventAttributesExtractors.SearchResultExtractor
import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}

import scala.collection.BufferedIterator
import java.time.LocalDateTime

case class CardSearchEvent(
    timestamp: Option[LocalDateTime],
    searchId: String,
    queriesTexts: Seq[String],
    relatedDocuments: Seq[String]
) extends Event

object CardSearchEvent extends EventParser {
    override def parse(
        bufferedIt: BufferedIterator[String],
        errorStatsAcc: ErrorStatsAccumulator
    ): Option[Event] = {
        if (!bufferedIt.hasNext) return None

        val eventName = "CARD_SEARCH_START"
        val firstLine = bufferedIt.next()

        if (!firstLine.startsWith(eventName)) {
            errorStatsAcc.add((
                "CardSearchInvalidFormat",
                s"Expected CARD_SEARCH_START line, got: ${firstLine.take(50)}..."
            ))
            return None
        }

        val splitFirstLine = firstLine.trim.split("\\s+")

        if (splitFirstLine.length < 2) {
            errorStatsAcc.add((
                "CardSearchMissingData",
                s"[$eventName] Not enough tokens in line: $firstLine"
            ))
            return None
        }

        val timestamp = DateTimeExtractor.extractTimestamp(splitFirstLine.last, eventName, errorStatsAcc)

        val queryLines = scala.collection.mutable.ListBuffer[String]()
        while (bufferedIt.hasNext && !bufferedIt.head.startsWith("CARD_SEARCH_END")) {
            val nextLine = bufferedIt.head
            if (nextLine.startsWith("DOC_OPEN") || nextLine.startsWith("SESSION_END")) {
                errorStatsAcc.add(("CardSearchUnexpectedLine", s"Expected CARD_SEARCH_END, got: $nextLine"))
            }
            queryLines += bufferedIt.next()
        }

        val queriesTexts = queryLines.flatMap(line =>
            if (line.startsWith("$")) {
                Some(line.stripPrefix("$"))
            } else {
                errorStatsAcc.add(("CardSearchMissingDollarPrefix", s"Unexpected line in card search: $line"))
                None
            }
        ).toSeq

        if (!bufferedIt.hasNext) {
            errorStatsAcc.add(("CardSearchMalformedEvent", "Missing CARD_SEARCH_END"))
            return Some(CardSearchEvent(
                timestamp = timestamp,
                searchId = "",
                queriesTexts = queriesTexts,
                relatedDocuments = Seq.empty
            ))
        }

        bufferedIt.next() // пропускаем CARD_SEARCH_END

        if (!bufferedIt.hasNext || bufferedIt.head.trim.charAt(0).isUpper) {
            errorStatsAcc.add((
                "CardSearchMissingSearchResult",
                s"[$eventName] Expected search result line after CARD_SEARCH_END"
            ))
            return Some(CardSearchEvent(
                timestamp = timestamp,
                searchId = "",
                queriesTexts = queriesTexts,
                relatedDocuments = Seq.empty
            ))
        }

        val (searchId, relatedDocuments) = SearchResultExtractor.extractSearchResult(bufferedIt, errorStatsAcc)

        Some(CardSearchEvent(
            timestamp = timestamp,
            searchId = searchId,
            queriesTexts = queriesTexts,
            relatedDocuments = relatedDocuments
        ))
    }
}