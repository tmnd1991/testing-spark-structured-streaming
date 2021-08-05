# testing-spark-structured-streaming

Who loves Apache Spark Structured Streaming? 🙋‍♂️

Who hates the fact that testing streaming queries is very difficult? 🙋‍♂️

That's why this library exists: to expose a nice and *pure* API to test Apache Spark Structured
Streaming queries.

It consists of test sources of Spark itself that I took and cleaned a bit, with this library you 
can do:

```scala
class TestTheQueryTest extends QueryTest with StreamTest {

  override val streamingTimeout: Span = 1000.minutes // to set after how long a test should fail

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
```

As you can see, the `testStream` part is just "pushing" events into the streaming query, 
manipulating the "clock" and asserting that some (or none!) record should be produced by the
streaming query, in that microbatch! Isn't this beautiful?


## Accessing the library

Add this to your sbt build:

```scala
libraryDependencies += "com.github.tmnd1991" %% "testing-spark-structured-streaming" % "0.0.1"
```

Or this to your pom.xml:

```xml
<dependency>
    <groupId>com.github.tmnd1991</groupId>
    <artifactId>testing-spark-structured-streaming_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```

And if you want to depend on some snapshot version, you need to add sonatype snapshot repository:

On sbt:

```scala
resolvers += Resolver.sonatypeRepo("snapshots")
```

On maven:

```xml
<repositories>
  <repository>
    <id>snapshots-repo</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
```

## How to contribute

Found a bug? ➡ Open an issue!

Want to contribute? ➡ Open a PR!