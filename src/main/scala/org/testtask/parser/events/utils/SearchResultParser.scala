package org.testtask.parser.events.utils

import org.testtask.parser.processors.ParsingContext

object SearchResultParser {
  def parserSearchResult(
      context: ParsingContext
  ): (String, Seq[String]) = {
    val lines = context.lines
    val errorStatsAcc = context.errorStatsAcc
    val fileName = context.fileName

    val line = lines.head

    if (line.trim.charAt(0).isUpper) {
      errorStatsAcc.add(
        (
          "Warning: UnexpectedEndOfSearch",
          s"[file $fileName] Expected search result line, got unexpected event start: $line"
        )
      )
      return ("", Seq.empty)
    }

    val fullLine = lines.next()
    val splitFullLine = fullLine.trim.split("\\s+")

    if (splitFullLine.length < 2) {
      errorStatsAcc.add(
        ("Warning: SearchDocumentsMissing", s"[file $fileName] No documents found in search line: $line")
      )
      return (fullLine.trim, Seq.empty)
    }

    val searchId = splitFullLine.head
    val relatedDocuments = splitFullLine.tail

    (searchId, relatedDocuments)
  }
}
