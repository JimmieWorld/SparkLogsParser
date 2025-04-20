package org.james_world

import scala.util.matching.Regex
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

object Extractors {
    private val documentPattern: Regex = "[A-Z]+_\\d+".r
    private val searchResultPattern: Regex = "(?<=\\s|^)-?\\d+\\s".r
    private val quickSearchQueryPattern: Regex = "\\{([^\\}]+)\\}".r

    object Timestamp {
        private type TimestampParser = String => Option[LocalDateTime]
        private val timestampPattern1: Regex = "\\d{2}\\.\\d{2}\\.\\d{4}_\\d{2}:\\d{2}:\\d{2}".r
        private val timestampPattern2: Regex = "[A-Za-z]{3},_\\d{1,2}_[A-Za-z]{3}_\\d{4}_\\d{2}:\\d{2}:\\d{2}_\\+\\d{4}".r

        private val parsers: List[TimestampParser] = List(
            parseFormat1,
            parseFormat2
        )

        def extractTimestamp(line: String): LocalDateTime = {
            parsers.view.flatMap(parser => parser(line)).headOption.getOrElse(LocalDateTime.of(0, 1, 1, 0, 0, 0))
        }

        private def parseFormat1(line: String): Option[LocalDateTime] = {
            timestampPattern1.findFirstIn(line).flatMap { timestampStr =>
                try {
                    Some(LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")))
                } catch {
                    case _: Exception => None
                }
            }
        }

        private def parseFormat2(line: String): Option[LocalDateTime] = {
            timestampPattern2.findFirstIn(line).flatMap { timestampStr =>
                try {
                    val zonedDateTime = ZonedDateTime.parse(timestampStr.replace("_", " "),
                        DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss Z"))
                    Some(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)
                } catch {
                    case _: Exception => None
                }
            }
        }
    }

    def extractDocumentId(line: String): String = {
        documentPattern.findFirstIn(line).getOrElse("")
    }

    def extractQueryId(line: String): String = {
        searchResultPattern.findFirstIn(line).map(_.trim).getOrElse("")
    }

    def extractDocuments(line: String): Seq[String] = {
        documentPattern.findAllIn(line).toSeq
    }

    def extractQuery(line: String): Option[String] = {
        if (line.startsWith("$")) Some(line.stripPrefix("$")) else None
    }

    def extractQuickSearchQuery(line: String): String = {
        quickSearchQueryPattern.findFirstIn(line).getOrElse("")
    }

}

trait EventParser {
    def parse(lines: Seq[String]): Option[Event]
}

object SessionStartParser extends EventParser {
    override def parse(line: Seq[String]): Option[Event] = {
        val timestamp = Extractors.Timestamp.extractTimestamp(line.head)

        Some(SessionStart(timestamp))
    }
}

object SessionEndParser extends EventParser {
    override def parse(line: Seq[String]): Option[Event] = {
        val timestamp = Extractors.Timestamp.extractTimestamp(line.head)

        Some(SessionEnd(timestamp))
    }
}

object CardSearchParser extends EventParser {
    override def parse(lines: Seq[String]): Option[Event] = {
        if (lines.isEmpty) return None

        val timestamp = Extractors.Timestamp.extractTimestamp(lines.head)
        val searchId = Extractors.extractQueryId(lines.last)
        val relatedDocuments = Extractors.extractDocuments(lines.last)
        val queriesTexts = lines.tail.flatMap(Extractors.extractQuery)

        Some(CardSearch(
            timestamp = timestamp,
            searchId = searchId,
            queriesTexts = queriesTexts,
            relatedDocuments = relatedDocuments
        ))
    }
}

object QuickSearchParser extends EventParser {
    override def parse(lines: Seq[String]): Option[Event] = {
        if (lines.isEmpty) return None

        val timestamp = Extractors.Timestamp.extractTimestamp(lines.head)
        val searchId = Extractors.extractQueryId(lines.last)
        val queryText = Extractors.extractQuickSearchQuery(lines.head)
        val relatedDocuments = Extractors.extractDocuments(lines.last)

        Some(QuickSearch(
            timestamp = timestamp,
            searchId = searchId,
            queryText = queryText,
            relatedDocuments = relatedDocuments
        ))
    }
}

object DocumentOpenParser extends EventParser {
    private var searchTimestamps: Map[String, LocalDateTime] = Map.empty

    override def parse(line: Seq[String]): Option[Event] = {
        if (line.isEmpty) return None

        val searchId = Extractors.extractQueryId(line.head)
        val documentId = Extractors.extractDocumentId(line.head)
        val rawTimestamp = Extractors.Timestamp.extractTimestamp(line.head)
        val timestamp = if (rawTimestamp == LocalDateTime.of(0, 1, 1, 0, 0, 0)) {
            searchTimestamps.getOrElse(searchId, LocalDateTime.of(0, 1, 1, 0, 0, 0))
        } else {
            rawTimestamp
        }

        Some(DocumentOpen(
            timestamp = timestamp,
            searchId = searchId,
            documentId = documentId
        ))
    }

    def addSearchTimestamp(timestamp: Map[String, LocalDateTime]): Unit = {
        searchTimestamps ++= timestamp
    }
}