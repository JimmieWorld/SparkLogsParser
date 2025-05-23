package org.james_world.events.utils

import org.james_world.ParsingContext

object SearchResultParser {
  def parserSearchResult(
      context: ParsingContext
  ): (String, Seq[String]) = {
    val line = context.bufferedIt.head

    if (line.trim.charAt(0).isUpper) {
      context.errorStatsAcc.add(
        ("Warning: UnexpectedEndOfSearch", s"Expected search result line, got unexpected event start: $line")
      )
      return ("", Nil)
    }

    val fullLine = context.bufferedIt.next()
    val splitFullLine = fullLine.trim.split("\\s+")

    if (splitFullLine.length < 2) {
      context.errorStatsAcc.add(("Warning: SearchDocumentsMissing", s"No documents found in search line: $line"))
      return (fullLine.trim, Nil)
    }

    val searchId = splitFullLine.head
    val relatedDocuments = splitFullLine.tail

    (searchId, relatedDocuments)
  }
}
