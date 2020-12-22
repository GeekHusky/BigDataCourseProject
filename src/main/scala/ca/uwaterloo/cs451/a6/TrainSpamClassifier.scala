package ca.uwaterloo.cs451.a6


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.math._
import scala.collection.mutable.Map

class TrainSpamClassifier(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle training instance", required = false)
  verify()
}


object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {
    val args = new TrainSpamClassifier(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)


  
    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()
    // This is the main learner:
    val delta = 0.002
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(),1)

    if (args.shuffle()) {
      val trained = textFile
          .map(line =>{
              // Parse input
              val tokens = line.split(' ')
              val docid = tokens(0)
              val isSpam = if (tokens(1)=="spam") 1d else 0d
              val features = tokens.drop(2).map(_.toInt)

              val shuffle_key = scala.util.Random.nextInt
              
              (shuffle_key, (docid, isSpam, features))
          })
          .sortByKey()
          .map( p => {(0,p._2)})
          .groupByKey(1)
          .flatMap( docs => {
            docs._2.foreach( doc => {
              // For each instance...
              val isSpam = doc._2
              val features = doc._3

              // Update the weights as follows:
              val score = spamminess(features)
              val prob = 1.0 / (1 + exp(-score))
              features.foreach(f => {
                if (w.contains(f)) {
                  w(f) += (isSpam - prob) * delta
                } else {
                  w(f) = (isSpam - prob) * delta
                }
              })
            })
            w
          })
      trained.saveAsTextFile(args.model())
    }
    else {
      val trained = textFile
          .map(line =>{
              // Parse input
              val tokens = line.split(' ')
              val docid = tokens(0)
              val isSpam = if (tokens(1)=="spam") 1d else 0d
              val features = tokens.drop(2).map(_.toInt)
              (0, (docid, isSpam, features))
          })
          .groupByKey(1)
          .flatMap( docs => {
            docs._2.foreach( doc => {
              // For each instance...
              val isSpam = doc._2
              val features = doc._3

              // Update the weights as follows:
              val score = spamminess(features)
              val prob = 1.0 / (1 + exp(-score))
              features.foreach(f => {
                if (w.contains(f)) {
                  w(f) += (isSpam - prob) * delta
                } else {
                  w(f) = (isSpam - prob) * delta
                }
              })
            })
            w
          })
      trained.saveAsTextFile(args.model())
    }
    
  }
}
