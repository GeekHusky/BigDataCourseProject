/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext, StateSpec, State, Time}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {

    def mappingFunction(batchTime: Time, key: String, value: Option[Int], state: State[Int]): Option[(String,Tuple3[Int, Long, Int])] = {
      // Use state.exists(), state.get(), state.update() and state.remove()
      // to manage state, and return the necessary string
      var timestamp = batchTime.milliseconds
      var prev_state = 0
      var cur_val = value.getOrElse(0)

      if (state.exists){
          prev_state = state.get
          if (cur_val >= 2*prev_state && cur_val >= 10 && key != "else"){
              val city = if (key=="goldman") "Goldman Sachs" else "Citigroup"
              
              println("Number of arrivals to "+city+" has doubled from "+prev_state+" to "+cur_val+" at "+timestamp+"!")
          }
      }
      state.update(cur_val)
      Some((key, (cur_val, timestamp, prev_state)))
    }

    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")
    val timestampCount = spark.sparkContext.longAccumulator("Timestamp Count")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val goldman_longitude = List(-74.0141012, -74.013777, -74.0141027, -74.0144185)
    val goldman_latitude = List(40.7152191, 40.7152275, 40.7138745, 40.7140753)

    val citigroup_longitude = List(-74.011869, -74.009867, -74.010140, -74.012083)
    val citigroup_latitude = List(40.7217236, 40.721493, 40.720053, 40.720267)

    val wc = stream
      .map(line => {
            val item = line.split(",")
            var longitude = item(10).toDouble
            var latitude = item(11).toDouble
            var dp_city = "else"
            if (item(0)=="green") {
                longitude = item(8).toDouble
                latitude = item(9).toDouble
            }
            if (longitude > citigroup_longitude.min && longitude < citigroup_longitude.max 
                    && latitude > citigroup_latitude.min && latitude < citigroup_latitude.max)
            {
                    dp_city = "citigroup"
            }
            else if (longitude > goldman_longitude.min && longitude < goldman_longitude.max 
                    && latitude > goldman_latitude.min && latitude < goldman_latitude.max)
            {
                    dp_city = "goldman"
            }
            (dp_city, 1)
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(StateSpec.function(mappingFunction _))
      .persist()

    val output_path = args.output()

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
      val timestamp = rdd.first()._2._2
      rdd.saveAsTextFile(output_path + "/" + "part-" + "%08d".format(timestamp.toInt))
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
