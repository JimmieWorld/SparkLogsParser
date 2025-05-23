package org.james_world.events

import org.james_world.ParsingContext
import org.james_world.events.utils.{DateTimeParser, SearchResultParser}

import java.time.LocalDateTime

case class CardSearch(
    timestamp: Option[LocalDateTime],
    searchId: String,
    queriesTexts: Seq[String],
    relatedDocuments: Seq[String],
    docOpens: Seq[DocumentOpen]
) extends Event

object CardSearch extends EventParser {
  override def parse(
      context: ParsingContext
  ): Event = {
    val firstLine = context.bufferedIt.next()

    val splitFirstLine = firstLine.trim.split("\\s+")

    val timestamp = DateTimeParser.parseTimestamp(splitFirstLine.last, context.errorStatsAcc)

    val queryLines = scala.collection.mutable.ListBuffer[String]()
    while (context.bufferedIt.hasNext && !context.bufferedIt.head.startsWith("CARD_SEARCH_END")) {
      val nextLine = context.bufferedIt.head
      if (nextLine.startsWith("DOC_OPEN") || nextLine.startsWith("SESSION_END")) {
        context.errorStatsAcc.add(("CardSearchUnexpectedLine", s"Expected CARD_SEARCH_END, got: $nextLine"))
      }
      queryLines += context.bufferedIt.next()
    }

    val queriesTexts = queryLines
      .flatMap(line =>
        if (line.startsWith("$")) {
          Some(line.stripPrefix("$"))
        } else {
          context.errorStatsAcc.add(("Warning: CardSearchMissingDollarPrefix", s"Unexpected line in card search: $line"))
          None
        }
      )
      .toSeq

    context.bufferedIt.next() // пропускаем CARD_SEARCH_END

    if (!context.bufferedIt.hasNext || context.bufferedIt.head.trim.charAt(0).isUpper) {
      context.errorStatsAcc.add(
        (
          "Warning: CardSearchMissingSearchResult",
          s"[CARD_SEARCH] Expected search result line after CARD_SEARCH_END"
        )
      )
      return CardSearch(
        timestamp = timestamp,
        searchId = "",
        queriesTexts = queriesTexts,
        relatedDocuments = Seq.empty,
        docOpens = Seq.empty
      )
    }

    val (searchId, relatedDocuments) = SearchResultParser.parserSearchResult(context)

    CardSearch(
      timestamp = timestamp,
      searchId = searchId,
      queriesTexts = queriesTexts,
      relatedDocuments = relatedDocuments,
      docOpens = Seq.empty
    )
  }
}
