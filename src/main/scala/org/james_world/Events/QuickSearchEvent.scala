package org.james_world.Events

import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}
import java.time.LocalDateTime

case class QuickSearchEvent(
    timestamp: LocalDateTime,
    searchId: String,
    queryText: String,
    relatedDocuments: Seq[String]
) extends Event

object QuickSearchEvent extends EventParser {
    override def parse(lines: Seq[String]): Option[QuickSearchEvent] = {
        if (lines.isEmpty) return None

        val timestamp = DateTimeExtractor.extractTimestamp(lines.head)
        val searchId = EventAttributesExtractors.extractQueryId(lines.last)
        val queryText = EventAttributesExtractors.extractQuickSearchQuery(lines.head)
        val relatedDocuments = EventAttributesExtractors.extractDocuments(lines.last)

        Some(QuickSearchEvent(
            timestamp = timestamp,
            searchId = searchId,
            queryText = queryText,
            relatedDocuments = relatedDocuments
        ))
    }
}