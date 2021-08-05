package com.github.tmnd1991.spark.testing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.{Encoders, QueryTest, Row}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

class TestTheQueryTest extends QueryTest with StreamTest with Logging {

  lazy val commediaDf = spark.read
    .text(getTestResourcePath("divinacommedia.txt"))
    .as[String](Encoders.STRING)

  override val streamingTimeout: Span = 1000.minutes

  test("Testing a query that count the lenght of the word given in input") {
    import spark.implicits._
    val staticInput = List(
      "Nel",
      "mezzo",
      "del",
      "cammin"
    )
    val source = MemoryStream[String](0, spark.sqlContext)
    val res = source.toDS().map(_.length)

    testStream(res)(
      StartStream(Trigger.ProcessingTime(10), new StreamManualClock),
      AddData(source, staticInput.head),  // passing the first element to the query ==> nel
      AdvanceManualClock(10),
      CheckNewAnswer(3),
      AddData(
        source,
        staticInput(1) // passing the second element to the query ==> mezzo
      ),
      AdvanceManualClock(10),
      CheckAnswer(5) // mezzo is composed by 5 letters

      // and so on, let's see another more complex example
    )
  }

  test("Testing a query that count the lenght of the word given in input") {
    import spark.implicits._
    val staticInput = List(
      "Nel",
      "mezzo",
      "del",
      "cammin",
      "di",
      "nostra",
      "vita",
      "mi",
      "ritrovai",
      "per",
      "una",
      "selva",
      "oscura",
      "ché",
      "la",
      "diritta",
      "via",
      "era",
      "smarrita."
    )
    val source = MemoryStream[String](0, spark.sqlContext)
    val res = source.toDS().map(_.length)

    testStream(res)(
      StartStream(Trigger.ProcessingTime(10), new StreamManualClock),
      AddData(source, staticInput.head),
      AdvanceManualClock(10),
      CheckNewAnswer(3),
      AddData(
        source,
        staticInput.drop(1): _*
      ),
      AdvanceManualClock(10),
      CheckAnswerRows(
        staticInput.drop(1).map(e => Row(e.length)), // checking the remaining answers
        lastOnly = true,
        isSorted = false
      )
    )
  }

  test("Dante more complex query") {
    import spark.implicits._
    val staticInput = commediaDf
      .take(3)
      .iterator
    val source = MemoryStream[String](0, spark.sqlContext)
    val res = source.toDS().map(_.split("\\s+").length).where(col("value").gt(lit(6)))
    testStream(res)(
      StartStream(Trigger.ProcessingTime(10), new StreamManualClock),
      AddData(source, staticInput.next()),
      AdvanceManualClock(10),
      CheckNewAnswer(7),
      AddData(
        source,
        staticInput.take(2).toList: _*
      ),
      AdvanceManualClock(10),
      CheckNewAnswer()
    )
  }

  test("streaming word counter") {
    import spark.implicits._
    val staticInput = commediaDf
      .take(100)
      .iterator

    val source = MemoryStream[String](0, spark.sqlContext)
    val clock = new StreamManualClock
    val timeout = 100L
    val trigger = 10L
    val res =
      source
        .toDS()
        .flatMap(_.split("\\s+"))
        .groupByKey(identity)
        .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout()) {
          (k: String, v: Iterator[String], s: GroupState[WordCounter]) =>
            val values = v.toList
            val now = s.getCurrentProcessingTimeMs()
            s.getOption match {
              case Some(oldState) if s.hasTimedOut =>
                val newState = oldState.copy(
                  count = oldState.count + values.size,
                  lastTrigger = now
                )
                s.update(newState)
                s.setTimeoutDuration(timeout)
                Iterator(k -> newState.count)
              case Some(oldState) =>
                val newState = oldState.copy(
                  count = oldState.count + values.size
                )
                s.update(newState)
                s.setTimeoutDuration(timeout - (now - newState.lastTrigger))
                List.empty[(String, Long)].iterator
              case None =>
                val newState = WordCounter(
                  count = values.size.toLong,
                  lastTrigger = now
                )
                s.update(newState)
                s.setTimeoutDuration(timeout - (now - newState.lastTrigger))
                List.empty[(String, Long)].iterator
            }
        }
    testStream(res)(
      StartStream(Trigger.ProcessingTime(trigger), clock),
      AddData(source, staticInput.next()), // we set timeout to 110 for the first batch
      SetManualClock(trigger),
      CheckNewAnswer(),
      SetManualClock(trigger * 11),
      AddData(source, staticInput.next()),
      SetManualClock(trigger * 12), // first states time out and then we get output, we next timeout ot 220
      CheckNewAnswer(
        "mezzo" -> 1,
        "Nel" -> 1,
        "cammin" -> 1,
        "vita" -> 1,
        "di" -> 1,
        "del" -> 1,
        "nostra" -> 1
      ),
      SetManualClock(trigger * 22), // first and second states time out and then we get other output
      AddData(source, staticInput.next()),
      SetManualClock(trigger * 23),
      CheckNewAnswer(
        "Nel" -> 1,
        "cammin" -> 1,
        "del" -> 1,
        "di" -> 1,
        "mezzo" -> 1,
        "mi" -> 1,
        "nostra" -> 1,
        "oscura," -> 1,
        "per" -> 1,
        "ritrovai" -> 1,
        "selva" -> 1,
        "una" -> 1,
        "vita" -> 1
      ),
      StopStream
    )
  }
}
