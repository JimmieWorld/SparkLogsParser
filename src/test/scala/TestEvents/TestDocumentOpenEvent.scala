import org.james_world.Events.DocumentOpenEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestDocumentOpenEvent extends AnyFlatSpec with Matchers {

    "DocumentOpenEvent.parse" should "correctly parse event with valid timestamp" in {
        val input = Seq("DOC_OPEN 11.08.2020_12:05:20 239407215 LAW_211576")

        val result = DocumentOpenEvent.parse(input)

        result shouldBe defined
        val event = result.get
        event.timestamp shouldEqual LocalDateTime.of(2020, 8, 11, 12, 5, 20)
        event.searchId shouldBe "239407215"
        event.documentId shouldBe "LAW_211576"
    }

    it should "use timestamp from searchTimestamps when no timestamp in input" in {
        DocumentOpenEvent.addSearchTimestamp(
            Map("239407215" -> LocalDateTime.of(2020, 8, 11, 12, 5, 20))
        )

        val input = Seq("DOC_OPEN  239407215 DOC_123")
        val result = DocumentOpenEvent.parse(input)

        result shouldBe defined
        result.get.timestamp shouldEqual LocalDateTime.of(2020, 8, 11, 12, 5, 20)
    }

    it should "return default timestamp when searchId not found in searchTimestamps" in {
        val input = Seq("DOC_OPEN  239407218 LAW_211576")
        val result = DocumentOpenEvent.parse(input)

        result shouldBe defined
        result.get.timestamp shouldEqual LocalDateTime.of(0, 1, 1, 0, 0, 0)
    }

    it should "parse invalid line and return default timestamp with empty fields" in {
        val input = Seq("DOC_OPEN This is a broken log line")
        val result = DocumentOpenEvent.parse(input)

        result shouldBe defined
        val event = result.get
        event.timestamp shouldEqual LocalDateTime.of(0, 1, 1, 0, 0, 0)
        event.searchId shouldBe ""
        event.documentId shouldBe ""
    }

    it should "merge new timestamps into searchTimestamps correctly" in {
        DocumentOpenEvent.addSearchTimestamp(
            Map("22223" -> LocalDateTime.of(2023, 1, 1, 10, 0))
        )
        DocumentOpenEvent.addSearchTimestamp(
            Map("22224" -> LocalDateTime.of(2023, 1, 2, 10, 0))
        )

        val result1 = DocumentOpenEvent.parse(Seq("DOC_OPEN  22223 LAW_211576"))
        val result2 = DocumentOpenEvent.parse(Seq("DOC_OPEN  22224 LAW_311578"))

        result1.get.timestamp shouldEqual LocalDateTime.of(2023, 1, 1, 10, 0)
        result2.get.timestamp shouldEqual LocalDateTime.of(2023, 1, 2, 10, 0)
    }
}