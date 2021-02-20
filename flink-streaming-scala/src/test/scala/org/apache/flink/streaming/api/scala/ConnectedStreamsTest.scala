package org.apache.flink.streaming.api.scala

import org.junit.{ Assert, Test }

class ConnectedStreamsTest {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  private val dataStream1 = env.fromElements("a1", "a2", "a3")
  private val dataStream2 = env.fromElements("a1", "a2")
  private val keySelector = (s: String) => s

  @Test
  def testStatefulCoMap(): Unit = {

    def mapper = (s: String, state: Int) => {
      val newState = state + 1
      ((s, newState), newState)
    }

    val out = dataStream1
      .connect(dataStream2)
      .keyBy(keySelector, keySelector)
      .coMapWithState[(String, Int), Int](
        mapper,
        mapper,
        emptyState = 0
      ).executeAndCollect()

    val expected = Set(("a1", 2), ("a2", 2), ("a3", 1))
    val collected = out.toList.groupBy(_._1).mapValues(_.last).values.toSet

    Assert.assertEquals(expected, collected)

    out.close()
  }

}
