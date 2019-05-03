package matthelm

import com.spotify.scio.streaming._
import com.spotify.scio.testing._
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.{IntervalWindow, _}
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}

class LeaderBoardTest extends PipelineSpec {

  case class GameActionInfo(user: String, team: String, score: Int, timestamp: Long)

  private val allowedLateness = Duration.standardHours(1)
  private val teamWindowDuration = Duration.standardMinutes(20)
  private val baseTime = new Instant(0)

  case class TestUser(user: String, team: String)
  private val redOne = TestUser("scarlet", "red")
  private val redTwo = TestUser("burgundy", "red")
  private val blueOne = TestUser("navy", "blue")
  private val blueTwo = TestUser("sky", "blue")

  private def event(user: TestUser,
                    score: Int,
                    baseTimeOffset: Duration): TimestampedValue[GameActionInfo] = {
    val t = baseTime.plus(baseTimeOffset)
    TimestampedValue.of(GameActionInfo(user.user, user.team, score, t.getMillis), t)
  }

  "LeaderBoard.calculateTeamScores" should "work with on time elements" in {
    val stream = testStreamOf[GameActionInfo]
      .advanceWatermarkTo(baseTime)
      .addElements(
        event(blueOne, 3, Duration.standardSeconds(3)),
        event(blueOne, 2, Duration.standardMinutes(1)),
        event(redTwo, 3, Duration.standardSeconds(22)),
        event(blueTwo, 5, Duration.standardSeconds(3)))
      .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
      .addElements(
        event(redOne, 1, Duration.standardMinutes(4)),
        event(blueOne, 2, Duration.standardSeconds(270)))
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val teamScores = sc.testStream(stream)
        .withFixedWindows(
          teamWindowDuration,
          options = WindowOptions(
            trigger = AfterWatermark
              .pastEndOfWindow()
              .withEarlyFirings(
                AfterProcessingTime
                  .pastFirstElementInPane()
                  .plusDelayOf(Duration.standardMinutes(5)))
              .withLateFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(10))),
            accumulationMode = ACCUMULATING_FIRED_PANES,
            allowedLateness = allowedLateness
          )
        )
        .map(i => (i.team, i.score))
        .sumByKey

      val window = new IntervalWindow(baseTime, teamWindowDuration)
      teamScores should inOnTimePane(window) {
        containInAnyOrder(Seq((blueOne.team, 12), (redOne.team, 4)))
      }
    }
  }

}
