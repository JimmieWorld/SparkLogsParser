package testTasks

import org.james_world.Session
import org.james_world.events.{CardSearch, DocumentOpen, QuickSearch}
import org.james_world.tasks.Task2
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, LocalDateTime}

class TestTask2 extends AnyFlatSpec with Matchers with TestSparkContext {

  val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
  val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

   "countDocumentOpens" should "correctly count document opens after quick search" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "s1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616", "DOC_123"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
              searchId = "s1",
              documentId = "ACC_45616"
            ),
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
              searchId = "s1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq(
        DocumentOpen(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
          searchId = "s2",
          documentId = "ACC_45616"
        )
      )
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))

    val expected = Seq(
      (LocalDate.of(2023, 8, 1), "ACC_45616", 2)
    )
    result shouldBe expected
  }

  it should "not count document opens with quick search" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "cs1",
          queriesTexts = Seq("$ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
              searchId = "cs1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))
    result shouldBe Seq.empty
  }

  it should "not count document opens without quick search" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "cs1",
          queriesTexts = Seq("$ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
              searchId = "cs1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "qs1",
          queryText = "",
          relatedDocuments = Seq.empty,
          docOpens = Seq.empty
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))
    result shouldBe empty
  }

  it should "count document opens across multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "s1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
              searchId = "s1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 13, 0)),
          searchId = "s2",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 13, 5)),
              searchId = "s2",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session1, session2)))

    val expected = Seq(
      (LocalDate.of(2023, 8, 1), "ACC_45616", 2)
    )

    result shouldBe expected
  }

  it should "count document opens on different dates and sessions separately" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "s1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
              searchId = "s1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 2, 12, 0)),
          searchId = "s2",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 2, 12, 5)),
              searchId = "s2",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session1, session2)))

    val expected = Seq(
      (LocalDate.of(2023, 8, 1), "ACC_45616", 1),
      (LocalDate.of(2023, 8, 2), "ACC_45616", 1)
    )

    result.toSet shouldBe expected.toSet
  }

  it should "count document opened after both QS and CS" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "cs1",
          queriesTexts = Seq("$ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
              searchId = "cs1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "qs1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
              searchId = "qs1",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))

    val expected = Seq(
      (LocalDate.of(2023, 8, 1), "ACC_45616", 1)
    )

    result shouldBe expected
  }

  it should "count document opened after multiple QS" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "qs1",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
              searchId = "qs1",
              documentId = "ACC_45616"
            )
          )
        ),
        QuickSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "qs2",
          queryText = "ACC_45616",
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
              searchId = "qs2",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))

    val expected = Seq(
      (LocalDate.of(2023, 8, 1), "ACC_45616", 2)
    )

    result shouldBe expected
  }

  it should "count document opened after multiple CS" in {
    val session = Session(
      sessionId = "session3",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          searchId = "cs1",
          queriesTexts = Seq("$ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
              searchId = "cs1",
              documentId = "ACC_45616"
            )
          )
        ),
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          searchId = "cs2",
          queriesTexts = Seq("$ACC_45616"),
          relatedDocuments = Seq("ACC_45616"),
          docOpens = Seq(
            DocumentOpen(
              timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
              searchId = "cs2",
              documentId = "ACC_45616"
            )
          )
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    val result = Task2.execute(sc.parallelize(Seq(session)))

    val expected = Seq.empty

    result shouldBe expected
  }
}
