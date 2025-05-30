package org.testtask.parser.events.utils

import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

object DateTimeParser {

  private val formats = Iterator(
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss"),
    DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss", Locale.ENGLISH)
  )

  def parseDateTime(
      line: String,
      context: ParsingContext
  ): Option[LocalDateTime] = {
    val dateLine = line
      .replaceAll("(_[+-]\\d{4})$", "")

    for (format <- formats) {
      Try(LocalDateTime.parse(dateLine, format)).toOption.foreach { dateTime =>
        return Some(dateTime)
      }
    }

    context.errorStats.add(
      (
        "Warning: InvalidTimestampFormat",
        s"[file ${context.fileName}] Failed to parse timestamp from line: $line"
      )
    )
    None
  }
}
