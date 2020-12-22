package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "pmi threshold", required = false, default = Some(0))
  verify()
}


object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val lineNum = sc.broadcast(textFile.count())
    val threshold = sc.broadcast(args.threshold())

    val uniqueCounts = textFile
      .flatMap(line => {
        if (line.length > 0) {
          tokenize(line).slice(0,40).distinct
        }
        else {
          List()
        }
      })
      .map(w => (w,1))
      .reduceByKey((_ + _),args.reducers())
      .collectAsMap()

    val countsBroadcast = sc.broadcast(uniqueCounts)
    val pairsPMI = textFile
      .flatMap(line => {
        if (line.length > 0) {
          val unique_word = tokenize(line).slice(0,40).distinct
          var final_pair : List[(String,String)] = List()
          for (w1 <- unique_word) {
            for (w2 <- unique_word ) {
              if (w1 != w2) {
                final_pair = final_pair :+ ((w1, w2)) 
              }
            }
          }
          final_pair
        }
        else {
          List()
        }
      })
      .map(p => (p,1))
      .reduceByKey((_ + _),args.reducers())
      .filter(p => p._2 >= threshold.value)
      .map(p => {     
        val w1_p=countsBroadcast.value(p._1._1)/lineNum.value.toFloat
        val w2_p=countsBroadcast.value(p._1._2)/lineNum.value.toFloat
        val p_p=p._2/lineNum.value.toFloat
        val pmi=Math.log10(p_p/(w1_p*w2_p))

        "(("+p._1._1+","+p._1._2+"),"+(pmi,p._2)+")"
      })
      
    pairsPMI.saveAsTextFile(args.output())
  }
}
