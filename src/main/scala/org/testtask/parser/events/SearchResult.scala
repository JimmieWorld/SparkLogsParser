package org.testtask.parser.events

import org.testtask.parser.processors.ParsingContext

case class SearchResult(
    searchId: String,
    relatedDocuments: Seq[String],
    var docOpens: Seq[DocumentOpen] = Seq.empty
)

object SearchResult {
  def parse(
      context: ParsingContext
  ): SearchResult = {
    var line = context.lines.head

    if (line.trim.charAt(0).isUpper) {
      context.errorStats.add(
        (
          "Warning: UnexpectedEndOfSearch",
          s"[file ${context.fileName}] Expected search result line, got unexpected event start: $line"
        )
      )
      return SearchResult(null, null)
    }

    line = context.lines.next()
    val splitLine = line.trim.split("\\s+")

    if (splitLine.length < 2) return SearchResult(line.trim, Seq.empty)

    val searchId = splitLine.head
    val relatedDocuments = splitLine.tail

    SearchResult(searchId, relatedDocuments)
  }
}
