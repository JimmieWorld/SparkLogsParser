package org.james_world.Events

import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}
import java.time.LocalDateTime

case class CardSearchEvent(
    timestamp: LocalDateTime,
    searchId: String,
    queriesTexts: Seq[String],
    relatedDocuments: Seq[String]
) extends Event

object CardSearchEvent extends EventParser {
    override def parse(lines: Seq[String]): Option[CardSearchEvent] = {
        if (lines.isEmpty) return None

        val timestamp = DateTimeExtractor.extractTimestamp(lines.head)
        val searchId = EventAttributesExtractors.extractQueryId(lines.last)
        val relatedDocuments = EventAttributesExtractors.extractDocuments(lines.last)
        val queriesTexts = lines.tail.flatMap(EventAttributesExtractors.extractQuery)

        Some(CardSearchEvent(
            timestamp = timestamp,
            searchId = searchId,
            queriesTexts = queriesTexts,
            relatedDocuments = relatedDocuments
        ))
    }
}