package testParser.testEvents.utils

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestDateTimeParser extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

    var context: ParsingContext = _

    override def beforeEach(): Unit = {
      val errorStats = mock(classOf[ErrorStatsAccumulator])
      context = ParsingContext(Iterator.empty[String].buffered, errorStats, "4")
      super.beforeEach()
    }

    "extractTimestamp" should "parse default format (dd.MM.yyyy_HH:mm:ss)" in {
        val line = "13.02.2020_21:45:55"
        val result = DateTimeParser.parseDateTime(line, context)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 45, 55)
        verifyZeroInteractions(context.errorStats)
    }

    it should "parse RFC822-like format for QS event" in {
        val line = "Thu,_13_Feb_2020_21:38:09_+0300"
        val result = DateTimeParser.parseDateTime(line, context)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 38, 9)
        verifyZeroInteractions(context.errorStats)
    }

    it should "fail on invalid date and log error" in {
        val line = "BAD_DATE_FORMAT"
        val result = DateTimeParser.parseDateTime(line, context)

        result shouldBe None
        verify(context.errorStats).add((
            "Warning: InvalidTimestampFormat",
            "[file 4] Failed to parse timestamp from line: BAD_DATE_FORMAT"
        ))
    }
}