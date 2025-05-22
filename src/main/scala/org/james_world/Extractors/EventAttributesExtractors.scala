package org.james_world.Extractors

import org.james_world.ErrorStatsAccumulator
import scala.collection.BufferedIterator

object EventAttributesExtractors {

    object SearchResultExtractor {
        def extractSearchResult(
            bufferedIt: BufferedIterator[String],
            errorStatsAcc: ErrorStatsAccumulator
        ): (String, Seq[String]) = {
            if (!bufferedIt.hasNext) {
                errorStatsAcc.add(("MissingSearchResult", "Expected search result line, but input ended."))
                return ("", Nil)
            }

            val line = bufferedIt.head

            if (line.startsWith("DOC_OPEN") || line.startsWith("SESSION_END")) {
                errorStatsAcc.add(("UnexpectedEndOfSearch",
                    s"Expected search result line, got unexpected event start: $line"))
                return ("", Nil)
            }

            val fullLine = bufferedIt.next()
            val splitFullLine = fullLine.trim.split("\\s+")

            if (splitFullLine.length < 2) {
                errorStatsAcc.add(("SearchDocumentsMissing", s"No documents found in search line: $line"))
            }

            val searchId = splitFullLine.head
            val relatedDocuments = splitFullLine.tail

            (searchId, relatedDocuments)
        }
    }
}