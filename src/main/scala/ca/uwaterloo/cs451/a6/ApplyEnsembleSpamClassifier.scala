package ca.uwaterloo.cs451.a6


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.math._
import scala.collection.Map

class ApplyEnsembleSpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "method average/voting", required = true)
  verify()
}


object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    val method = args.method()
    // This is the main learner:
    val delta = 0.002
        
    val w_x_broadRDD= sc.broadcast(
         sc.textFile(args.model()+"/part-00000")
        .map(line => {
            val tokens = line.substring(1,line.length()-1).split(',')
            (tokens(0).toInt, tokens(1).toDouble)
        })
        .collectAsMap())
    val w_y_broadRDD= sc.broadcast(
         sc.textFile(args.model()+"/part-00001")
        .map(line => {
            val tokens = line.substring(1,line.length()-1).split(',')
            (tokens(0).toInt, tokens(1).toDouble)
        })
        .collectAsMap())
    val w_britney_broadRDD= sc.broadcast(
         sc.textFile(args.model()+"/part-00002")
        .map(line => {
            val tokens = line.substring(1,line.length()-1).split(',')
            (tokens(0).toInt, tokens(1).toDouble)
        })
        .collectAsMap())
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score_x = 0d
      var score_y = 0d
      var score_britney = 0d
      var score = 0d

      features.foreach(f => {
            if (w_x_broadRDD.value.contains(f)) score_x += w_x_broadRDD.value(f)
            if (w_y_broadRDD.value.contains(f)) score_y += w_y_broadRDD.value(f)
            if (w_britney_broadRDD.value.contains(f)) score_britney += w_britney_broadRDD.value(f)
      })
      if (method == "average"){
        score = (score_x + score_y + score_britney)/3
      }
      else {
        val vote_x = if (score_x > 0) 1d else -1d
        val vote_y = if (score_y > 0) 1d else -1d
        val vote_britney = if (score_britney > 0) 1d else -1d

        score = vote_x + vote_y + vote_britney
      }
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
