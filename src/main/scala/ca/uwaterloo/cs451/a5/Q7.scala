package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q7Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
    val file_name_1="lineitem"
    val file_name_2="orders"
    val file_name_3="customer"

    
    if (args.text()) {
        val customer = sc.textFile(args.input()+"/"+file_name_3+".tbl")
            // (c_custkey, c_name)
            .map(line => (line.split('|')(0).toInt,line.split('|')(1)))
            .collectAsMap()
        val customerBrod = sc.broadcast(customer)

        val orders = sc.textFile(args.input()+"/"+file_name_2+".tbl")
            // (o_orderkey, (o_custkey,o_orderdate,o_shippriority))
            .map(line => (line.split('|')(0).toInt,(line.split('|')(1).toInt,line.split('|')(4).toString,line.split('|')(7).toInt)))
            .filter(p => p._2._2 < date_parameter)

        val lineitem = sc.textFile(args.input()+"/"+file_name_1+".tbl")
            .filter(line => line.split('|')(10) > date_parameter)
            .map(line => {
                val fields=line.split('|')
                val orderkey=fields(0).toInt

                val extendedprice=fields(5).toDouble
                val discount=fields(6).toDouble

                val revenue=extendedprice*(1-discount)

                (orderkey,revenue)
            })
            .reduceByKey(_+_)
            .cogroup(orders)
            .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
            .map(p => {
                val custkey=p._2._2.head._1
                val orderdate=p._2._2.head._2
                val shippriority=p._2._2.head._3

                val custname=customerBrod.value.get(custkey).get

                (p._2._1.head,(custname,p._1,orderdate,shippriority))
            })
            .sortByKey(false)
            .take(10)
            .foreach{p => println("("+p._2._1+","+p._2._2+","+p._1+","+p._2._3+","+p._2._4+")")}
    }
    else {

         val customerRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_3)
            .rdd
            // (c_custkey, c_name)
            .map(fields => (fields.getInt(0),fields.getString(1)))
            .collectAsMap()
        val customerRDDBrod = sc.broadcast(customerRDD)

        val ordersRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_2)
            .rdd
            // (o_orderkey, (o_custkey,o_orderdate,o_shippriority))
            .map(fields => (fields.getInt(0),(fields.getInt(1),fields.getString(4),fields.getInt(7))))
            .filter(p => p._2._2 < date_parameter)


        val lineitemRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_1)
            .rdd
            .filter(line => line.getString(10) > date_parameter)
            .map(fields => {
                val orderkey=fields.getInt(0)

                val extendedprice=fields.getDouble(5)
                val discount=fields.getDouble(6)

                val revenue=extendedprice*(1-discount)

                (orderkey,revenue)
            })
            .reduceByKey(_+_)
            .cogroup(ordersRDD)
            .filter(p => p._2._1.size != 0 && p._2._2.size != 0)
            .map(p => {
                val custkey=p._2._2.head._1
                val orderdate=p._2._2.head._2
                val shippriority=p._2._2.head._3

                val custname=customerRDDBrod.value.get(custkey).get

                (p._2._1.head,(custname,p._1,orderdate,shippriority))
            })
            .sortByKey(false)
            .take(10)
            .foreach{p => println("("+p._2._1+","+p._2._2+","+p._1+","+p._2._3+","+p._2._4+")")}
    }


  }
}
