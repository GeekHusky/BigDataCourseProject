package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
    val file_name="lineitem"
    
    if (args.text()) {
        val counts = sc.textFile(args.input()+"/"+file_name+".tbl")
            .map(line => (line.split('|')(10),1))
            .filter(p => p._1 == date_parameter)
            .reduceByKey((a,b) => a+b)
            .collect().foreach(line => println("ANSWER="+line._2))
    }
    else {
        val lineitemRDD = SparkSession.builder.getOrCreate
                            .read.parquet(args.input()+"/"+file_name)
                            .rdd

        val counts = lineitemRDD
            .map(line => (line.getString(10),1))
            .filter(p => p._1 == date_parameter)
            .reduceByKey((a,b) => a+b)
            .collect().foreach(line => println("ANSWER="+line._2))
    }
  }
}
