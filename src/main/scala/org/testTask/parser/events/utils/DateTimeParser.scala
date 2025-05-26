package org.testTask.parser.events.utils

import org.testTask.parser.processors.ErrorStatsAccumulator
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.util.Try

object DateTimeParser {

  private val defaultFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val secondaryFormat = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss", Locale.ENGLISH)

  def parseTimestamp(
      line: String,
      errorStatsAcc: ErrorStatsAccumulator,
      fileName: String
  ): Option[LocalDateTime] = {
    val dateLine = line.trim
      .replaceAll("(_[+-]\\d{4})$", "")
      .trim

    if (Try(LocalDateTime.parse(dateLine, defaultFormat)).isSuccess) {
      Some(LocalDateTime.parse(dateLine, defaultFormat))
    } else if (Try(LocalDateTime.parse(dateLine, secondaryFormat)).isSuccess) {
      Some(LocalDateTime.parse(dateLine, secondaryFormat))
    } else {
      errorStatsAcc.add(
        (
          "Warning: InvalidTimestampFormat",
          s"[file $fileName] Failed to parse timestamp from line: $line"
        )
      )
      None
    }
  }
}
