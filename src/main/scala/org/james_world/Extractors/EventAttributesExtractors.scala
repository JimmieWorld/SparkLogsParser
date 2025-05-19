package org.james_world.Extractors

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

object EventAttributesExtractors {
    private val documentPattern: Regex = "[A-Z]+_\\d+".r
    private val searchResultPattern: Regex = "(?<=\\s|^)-?\\d+\\s".r
    private val quickSearchQueryPattern: Regex = "\\{([^\\}]+)\\}".r

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
        quickSearchQueryPattern.findFirstMatchIn(line) match {
            case Some(m) => m.group(1).trim
            case None => line.split("\\s+").drop(1).mkString(" ").trim
        }
    }
}