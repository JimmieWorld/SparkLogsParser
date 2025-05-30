package org.testtask.parser.events

import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

trait Event {
  def dateTime: Option[LocalDateTime]
}

trait EventParser {
  def keys(): Array[String]
  def parse(context: ParsingContext): Unit
}
