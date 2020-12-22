package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
    val file_name_1="lineitem"
    val file_name_2="orders"
    
    if (args.text()) {
        val lineitem = sc.textFile(args.input()+"/"+file_name_1+".tbl")
                .map(line => (line.split('|')(0).toInt,line.split('|')(10)))
                .filter(p => p._2 == date_parameter)

        val orders = sc.textFile(args.input()+"/"+file_name_2+".tbl")
                .map(line => (line.split('|')(0).toInt,line.split('|')(6)))

        val line_join_order = lineitem.cogroup(orders)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val red=p._2._1.size
                var final_pair : List[(Int,String)] = List()
                for (i <- 1 to red) {
                    final_pair = final_pair :+ ((p._1, p._2._2.head))
                }
                final_pair
            })
            .sortByKey()
            .take(20)
            .foreach(line => println("("+line._2+","+line._1+")"))
    }
    else {

        val lineitemRDD = SparkSession.builder.getOrCreate
                .read.parquet(args.input()+"/"+file_name_1)
                .rdd
                .map(line => (line.getInt(0),line.getString(10)))
                .filter(p => p._2 == date_parameter)

        val ordersRDD = SparkSession.builder.getOrCreate
                        .read.parquet(args.input()+"/"+file_name_2)
                        .rdd
                        .map(line => (line.getInt(0),line.getString(6)))

        val line_join_order = lineitemRDD.cogroup(ordersRDD)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val red=p._2._1.size
                var final_pair : List[(Int,String)] = List()
                for (i <- 1 to red) {
                    final_pair = final_pair :+ ((p._1, p._2._2.head))
                }
                final_pair
            })
            .sortByKey()
            .take(20)
            .foreach(line => println("("+line._2+","+line._1+")"))
    }


  }
}
