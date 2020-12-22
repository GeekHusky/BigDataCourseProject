package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "pmi threshold", required = false, default = Some(0))
  verify()
}


object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val lineNum = sc.broadcast(textFile.count())
    val threshold = sc.broadcast(args.threshold())
    
    val uniqueCounts = textFile
      .flatMap(line => {
        // if (line.length > 0) {
        tokenize(line).slice(0,40).distinct
        // }
      })
      .map(w => (w,1))
      .reduceByKey((_ + _),args.reducers())
      .collectAsMap()

    val countsBroadcast = sc.broadcast(uniqueCounts)

    val stripePMI = textFile
      .flatMap(line => {
        // if (line.length > 0) {
        //   val unique_word = tokenize(line).slice(0,40).distinct
        //   var final_stripe : List[(String,String)] = List()
        //   for (w1 <- unique_word) {
        //     for (w2 <- unique_word ) {
        //       if (w1 != w2) {
        //         final_stripe = final_stripe :+ ((w1, w2)) 
        //       }
        //     }
        //   }
        //   final_stripe.map(p => (p._1, Map(p._2 -> 1)))
        // } else List()
        if (line.length > 0) {
          val unique_word = tokenize(line).slice(0,40).distinct
          var final_stripe : List[(String,Map[String,Int])] = List()
          for (w1 <- unique_word) {
            final_stripe = final_stripe :+ ((w1, unique_word.filter(w=> w!=w1).toList.foldLeft(Map[String,Int]()) { (before,after) => before + (after -> 1) })) 
          }
          final_stripe

        } else List()
      })
      .reduceByKey( (m1,m2) => {
        m1++m2.map(p => (p._1 -> (p._2+m1.getOrElse(p._1,0))))
      },args.reducers())
      .map( s => {
        (s._1, s._2.filter( m => m._2 >= threshold.value))
      })
      .filter( s => s._2.size != 0)
      .map(stripe => {
        val w1_p=countsBroadcast.value(stripe._1)/lineNum.value.toFloat
        val strip_pairs = stripe._2.map(w => {
            val w2_p=countsBroadcast.value(w._1)/lineNum.value.toFloat
            val p_p=w._2/lineNum.value.toFloat
            val pmi=Math.log10(p_p/(w1_p*w2_p))
            (w._1 -> (pmi,w._2))
        })
        (stripe._1, strip_pairs)
      })
      
    stripePMI.saveAsTextFile(args.output())
  }
}
