package org.testtask.parser.events

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

case class CardSearch(
    timestamp: Option[LocalDateTime],
    queriesTexts: Seq[String],
    searchResult: SearchResult
) extends Event

object CardSearch extends EventParser {

  override def keys(): Array[String] = Array("CARD_SEARCH_START")

  override def parse(
      context: ParsingContext
  ): Unit = {
    val firstLine = context.lines.next()

    val splitFirstLine = firstLine.trim.split("\\s+")

    val timestamp = DateTimeParser.parseDateTime(splitFirstLine.last, context)

    val queryLines = scala.collection.mutable.ListBuffer[String]()
    while (context.lines.hasNext && !context.lines.head.startsWith("CARD_SEARCH_END")) {
      val nextLine = context.lines.head
      if (nextLine.startsWith("DOC_OPEN") || nextLine.startsWith("SESSION_END")) {
        context.errorStats.add(
          ("CardSearchUnexpectedLine", s"[file ${context.fileName}] Expected CARD_SEARCH_END, got: $nextLine")
        )
      }
      queryLines += context.lines.next()
    }

    val queriesTexts = queryLines
      .flatMap(line =>
        if (line.startsWith("$")) {
          Some(line.stripPrefix("$"))
        } else {
          context.errorStats.add(
            (
              "Warning: CardSearchMissingDollarPrefix",
              s"[file ${context.fileName}] Unexpected line in card search: $line"
            )
          )
          None
        }
      )
      .toSeq

    context.lines.next() // пропускаем CARD_SEARCH_END

    val searchResult = SearchResult.parse(context)

    context.sessionBuilder.cardSearches :+= CardSearch(timestamp, queriesTexts, searchResult)
  }
}
