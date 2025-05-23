package testTasks

import org.james_world.Session
import org.james_world.events.{CardSearch, DocumentOpen, QuickSearch}
import org.james_world.tasks.Task1
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestTask1 extends AnyFlatSpec with Matchers with TestSparkContext {

  val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
  val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

  "countSearchesForDocument" should "count only matching CardSearchEvents in multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "cs1",
          queriesTexts = Seq("ACC_45617"),
          relatedDocuments = Seq.empty,
          docOpens = Seq.empty
        ),
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "cs2",
          queriesTexts = Seq("ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 2, 10, 0)),
          searchId = "cs3",
          queriesTexts = Seq("DOC_789"),
          relatedDocuments = Seq("DOC_789"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val result = Task1.execute(sc.parallelize(Seq(session1, session2)), "ACC_45616")

    result shouldBe 1
  }

  it should "return zero if no matching documents found" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          searchId = "cs1",
          queriesTexts = Seq("DOC_123", "DOC_456"),
          relatedDocuments = Seq("DOC_123"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq(
        DocumentOpen(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          searchId = "qs1",
          documentId = "ACC_45616"
        )
      )
    )

    val result = Task1.execute(sc.parallelize(Seq(session)), "ACC_45616")
    result shouldBe 0
  }

  it should "ignore unrelated query texts and other event types" in {
    val session = Session(
      sessionId = "session3",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          searchId = "cs1",
          queriesTexts = Seq("NOT_ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          searchId = "qs1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq.empty
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task1.execute(sc.parallelize(Seq(session)), "ACC_45616")
    result shouldBe 0
  }

  it should "count correctly across multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          searchId = "cs1",
          queriesTexts = Seq("ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          searchId = "cs2",
          queriesTexts = Seq("ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq.empty
        ),
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          searchId = "cs3",
          queriesTexts = Seq("ACC_45616", "DOC_123"),
          relatedDocuments = Seq("DOC_123"),
          docOpens = Seq.empty
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val result = Task1.execute(sc.parallelize(Seq(session1, session2)), "ACC_45616")
    result shouldBe 3
  }
}