package ca.uwaterloo.cs451.a6


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.math._
import scala.collection.Map

class ApplySpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}


object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {
    val args = new ApplySpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
  
    // This is the main learner:
    val delta = 0.002
        
    val w_broadRDD= sc.broadcast(
         sc.textFile(args.model()+"/part-00000")
        .map(line => {
            val tokens = line.substring(1,line.length()-1).split(',')
            (tokens(0).toInt, tokens(1).toDouble)
        })
        .collectAsMap())
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w_broadRDD.value.contains(f)) score += w_broadRDD.value(f))
      score
    }
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val test = sc.textFile(args.input())
        .map(line =>{
            // Parse input
            val tokens = line.split(' ')
            val docid = tokens(0)
            val isSpam = tokens(1)
            val features = tokens.drop(2).map(_.toInt)

            val score = spamminess(features)
            val prediction = if (score > 0) "spam" else "ham"
            (docid, isSpam, score, prediction)
        })

    test.saveAsTextFile(args.output())
  }
}
