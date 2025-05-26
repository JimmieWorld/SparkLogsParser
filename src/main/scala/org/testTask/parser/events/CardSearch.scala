package org.testTask.parser.events

import org.testTask.parser.events.utils.{DateTimeParser, SearchResultParser}
import org.testTask.parser.processors.ParsingContext

import java.time.LocalDateTime

case class CardSearch(
    timestamp: Option[LocalDateTime],
    searchId: String,
    queriesTexts: Seq[String],
    relatedDocuments: Seq[String],
    docOpens: Seq[DocumentOpen] = Seq.empty
) extends Event

object CardSearch extends EventParser {
  override def parse(
      context: ParsingContext
  ): Event = {
    val lines = context.lines
    val errorStatsAcc = context.errorStatsAcc
    val fileName = context.fileName

    val firstLine = lines.next()

    val splitFirstLine = firstLine.trim.split("\\s+")

    val timestamp = DateTimeParser.parseTimestamp(splitFirstLine.last, errorStatsAcc, fileName)

    val queryLines = scala.collection.mutable.ListBuffer[String]()
    while (lines.hasNext && !lines.head.startsWith("CARD_SEARCH_END")) {
      val nextLine = lines.head
      if (nextLine.startsWith("DOC_OPEN") || nextLine.startsWith("SESSION_END")) {
        errorStatsAcc.add(("CardSearchUnexpectedLine", s"[file $fileName] Expected CARD_SEARCH_END, got: $nextLine"))
      }
      queryLines += lines.next()
    }

    val queriesTexts = queryLines
      .flatMap(line =>
        if (line.startsWith("$")) {
          Some(line.stripPrefix("$"))
        } else {
          errorStatsAcc.add(("Warning: CardSearchMissingDollarPrefix", s"[file $fileName] Unexpected line in card search: $line"))
          None
        }
      )
      .toSeq

    lines.next() // пропускаем CARD_SEARCH_END

    if (!lines.hasNext || lines.head.trim.charAt(0).isUpper) {
      errorStatsAcc.add(
        (
          "Warning: CardSearchMissingSearchResult",
          s"[file $fileName] Expected search result line after CARD_SEARCH_END"
        )
      )
      return CardSearch(
        timestamp = timestamp,
        searchId = "",
        queriesTexts = queriesTexts,
        relatedDocuments = Seq.empty
      )
    }

    val (searchId, relatedDocuments) = SearchResultParser.parserSearchResult(context)

    CardSearch(
      timestamp = timestamp,
      searchId = searchId,
      queriesTexts = queriesTexts,
      relatedDocuments = relatedDocuments
    )
  }
}
