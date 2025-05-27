package org.testtask.parser.events

import org.testtask.parser.events.utils.{DateTimeParser, SearchResultParser}
import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

case class CardSearch(
    timestamp: Option[LocalDateTime],
    queriesTexts: Seq[String],
    searchResult: SearchResult
) extends Event

object CardSearch extends EventParser {
  override def parse(
      context: ParsingContext
  ): Unit = {
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
          errorStatsAcc.add(
            ("Warning: CardSearchMissingDollarPrefix", s"[file $fileName] Unexpected line in card search: $line")
          )
          None
        }
      )
      .toSeq

    lines.next() // пропускаем CARD_SEARCH_END

    val searchResult = SearchResult.parse(context)

    context.sessionBuilder.cardSearches :+= CardSearch(timestamp, queriesTexts, searchResult)
  }
}
