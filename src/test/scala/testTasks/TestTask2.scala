package testTasks

import org.testtask.parser.events.{CardSearch, DocumentOpen, QuickSearch, SearchResult}
import org.testtask.parser.Session
import org.testtask.tasks.Task2
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime

class TestTask2 extends AnyFlatSpec with Matchers with TestSparkContext {

  val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
  val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

  "Task2.execute" should "correctly count document opens after quick search" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "s1",
            Seq("ACC_45616", "DOC_123"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                searchId = "s1",
                documentId = "ACC_45616"
              ),
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                searchId = "s1",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      docOpens = Seq(
        DocumentOpen(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
          searchId = "s2",
          documentId = "ACC_45616"
        )
      )
    )

    Task2.execute(sc.parallelize(Seq(session)))

    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))
    lines.size shouldBe 1

    lines.toArray shouldBe Array("2023-08-01,ACC_45616,2")
  }

  it should "not count document opens with quick search" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queriesTexts = Seq("$ACC_45616"),
          searchResult = SearchResult(
            "cs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                searchId = "cs1",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session)))
    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))
    lines.toArray shouldBe Array.empty
  }

  it should "not count document opens without quick search" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queriesTexts = Seq("$ACC_45616"),
          searchResult = SearchResult(
            "cs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                searchId = "cs1",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queryText = "",
          searchResult = SearchResult("qs1", Seq.empty)
        )
      ),
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session)))

    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))
    lines.toArray shouldBe Array.empty
  }

  it should "count document opens across multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "s1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                searchId = "s1",
                documentId = "ACC_45616"
              )
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
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 13, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "s2",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 13, 5)),
                searchId = "s2",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session1, session2)))

    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))

    lines.size shouldBe 1
    lines.toArray shouldBe Array("2023-08-01,ACC_45616,2")
  }

  it should "count document opens on different dates and sessions separately" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "s1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                searchId = "s1",
                documentId = "ACC_45616"
              )
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
          dateTime = Some(LocalDateTime.of(2023, 8, 2, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "s2",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 2, 12, 5)),
                searchId = "s2",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session1, session2)))
    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))

    lines.size shouldBe 2
    lines.toArray shouldBe Array(
      "2023-08-02,ACC_45616,1",
      "2023-08-01,ACC_45616,1"
    )
  }

  it should "count document opened after both QS and CS" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queriesTexts = Seq("ACC_45616"),
          searchResult = SearchResult(
            "cs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                searchId = "cs1",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "qs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                searchId = "qs1",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session)))
    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))

    lines.size shouldBe 1
    lines.toArray shouldBe Array("2023-08-01,ACC_45616,1")
  }

  it should "count document opened after multiple QS" in {
    val session = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq.empty,
      quickSearches = Seq(
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "qs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                searchId = "qs1",
                documentId = "ACC_45616"
              )
            )
          )
        ),
        QuickSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queryText = "ACC_45616",
          searchResult = SearchResult(
            "qs2",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                searchId = "qs2",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session)))
    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))

    lines.size shouldBe 1
    lines.toArray shouldBe Array("2023-08-01,ACC_45616,2")
  }

  it should "count document opened after multiple CS" in {
    val session = Session(
      sessionId = "session3",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queriesTexts = Seq("ACC_45616"),
          searchResult = SearchResult(
            "cs1",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                searchId = "cs1",
                documentId = "ACC_45616"
              )
            )
          )
        ),
        CardSearch(
          dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queriesTexts = Seq("ACC_45616"),
          searchResult = SearchResult(
            "cs2",
            Seq("ACC_45616"),
            docOpens = Seq(
              DocumentOpen(
                dateTime = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                searchId = "cs2",
                documentId = "ACC_45616"
              )
            )
          )
        )
      ),
      quickSearches = Seq.empty,
      docOpens = Seq.empty
    )

    Task2.execute(sc.parallelize(Seq(session)))
    val lines = Files.readAllLines(Paths.get("src/main/resources/results/task2_result.csv"))

    lines.toArray shouldBe Array.empty
  }
}
