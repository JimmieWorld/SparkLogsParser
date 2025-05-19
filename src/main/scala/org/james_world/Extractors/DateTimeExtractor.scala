package org.james_world.Extractors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.matching.Regex
import scala.util.{Success, Try}

object DateTimeExtractor {
    private case class FormatDefinition(pattern: Regex, formatter: DateTimeFormatter)

    private val formats: List[FormatDefinition] = List(
        FormatDefinition("""[A-Z][a-z]{2}, \d{1,2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2}""".r,
            DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss", Locale.ENGLISH)),
        FormatDefinition("""\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}""".r,
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")),
        FormatDefinition("""\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}""".r,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
        FormatDefinition("""\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}""".r,
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")),
        FormatDefinition("""\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}""".r,
            DateTimeFormatter.ofPattern("dd.MM.yy HH:mm:ss"))
    )

    def extractTimestamp(line: String): LocalDateTime = {
        val cleaned = line
            .trim
            .replaceAll("([+-]\\d{4})$", "")
            .replaceAll("_", " ")
            .trim

        val dateStr = formats
            .view
            .flatMap { format =>
                val pattern = format.pattern
                pattern.findFirstIn(cleaned)
            }
            .headOption

        dateStr match {
            case Some(datePart) =>
                formats
                    .view
                    .map(fd => Try(LocalDateTime.parse(datePart, fd.formatter)))
                    .collectFirst { case Success(dt) => dt }
                    .getOrElse(LocalDateTime.of(0, 1, 1, 0, 0, 0))
            case _ =>
                LocalDateTime.of(0, 1, 1, 0, 0, 0)
        }
    }
}