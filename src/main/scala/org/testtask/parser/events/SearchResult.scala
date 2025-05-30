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
    val line = context.lines.head

    if (line.trim.charAt(0).isUpper) {
      context.errorStats.add(
        (
          "Warning: UnexpectedEndOfSearch",
          s"[file ${context.fileName}] Expected search result line, got unexpected event start: $line"
        )
      )
      return SearchResult("", Seq.empty)
    }

    val fullLine = context.lines.next()
    val splitFullLine = fullLine.trim.split("\\s+")

    if (splitFullLine.length < 2) {
      context.errorStats.add(
        ("Warning: SearchDocumentsMissing", s"[file ${context.fileName}] No documents found in search line: $line")
      )

      return SearchResult(fullLine.trim, Seq.empty)
    }

    val searchId = splitFullLine.head
    val relatedDocuments = splitFullLine.tail

    SearchResult(searchId, relatedDocuments)
  }
}
