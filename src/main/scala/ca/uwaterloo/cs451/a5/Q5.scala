package ca.uwaterloo.cs451.a5

import scala.reflect.io.Directory
import java.io.File

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)
    val file_name_1="lineitem"
    val file_name_2="orders"
    val file_name_3="customer"
    val file_name_4="nation"

    
    if (args.text()) {
        val customer = sc.textFile(args.input()+"/"+file_name_3+".tbl")
            // (c_custkey, c_nationkey)
            .map(line => (line.split('|')(0).toInt,line.split('|')(3).toInt))
            .collectAsMap()
        val nation = sc.textFile(args.input()+"/"+file_name_4+".tbl")
            // (n_nationkey, n_name)
            .map(line => (line.split('|')(0).toInt,line.split('|')(1)))
            .collectAsMap()
        val customerBrod = sc.broadcast(customer)
        val nationBrod = sc.broadcast(nation)

        val orders = sc.textFile(args.input()+"/"+file_name_2+".tbl")
            // (o_orderkey, o_custkey)
            .map(line => (line.split('|')(0).toInt,line.split('|')(1).toInt))

        val lineitem = sc.textFile(args.input()+"/"+file_name_1+".tbl")
            // (l_orderkey, l_shipdate)
            .map(line => (line.split('|')(0).toInt,line.split('|')(10).substring(0,7)))
            // .filter(p => p._2 == date_parameter)
            .cogroup(orders)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val date=p._2._1.toList
                var final_pair : List[((Int,String,String),Int)] = List()
                for (d <- date) {
                    var nationID=customerBrod.value.get(p._2._2.head).get
                    var nationName=nationBrod.value.get(nationID).get
                    final_pair = final_pair :+ (((nationID,nationName,d),1))
                }
                final_pair
            })
            .filter( p => p._1._1 == 3 || p._1._1 == 24 )
            .reduceByKey(_+_,1)
            .sortByKey()
            .map( p => (p._1._1,p._1._2,p._1._3,p._2))

            lineitem.collect()
            .foreach{println}
    
            // lineitem
            // .map(p => p._1+","+p._2+","+p._3+","+p._4)
            // .saveAsTextFile("Q5-Text.csv")
    }
    else {

         val customerRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_3)
            .rdd
            .map(line => (line.getInt(0),line.getInt(3)))
            .collectAsMap()
        val nationRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_4)
            .rdd
            .map(line => (line.getInt(0),line.getString(1)))
            .collectAsMap()

        val customerBrodRDD = sc.broadcast(customerRDD)
        val nationBrodRDD = sc.broadcast(nationRDD)

        val ordersRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_2)
            .rdd
            // (o_orderkey, o_custkey)
            .map(line => (line.getInt(0),line.getInt(1)))

        val lineitemRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_1)
            .rdd
            .map(line => (line.getInt(0),line.getString(10).substring(0,7)))
            .cogroup(ordersRDD)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val date=p._2._1.toList
                var final_pair : List[((Int,String,String),Int)] = List()
                for (d <- date) {
                    var nationID=customerBrodRDD.value.get(p._2._2.head).get
                    var nationName=nationBrodRDD.value.get(nationID).get
                    final_pair = final_pair :+ (((nationID,nationName,d),1))
                }
                final_pair
            })
            .filter( p => p._1._1 == 3 || p._1._1 == 24 )
            .reduceByKey(_+_,1)
            .sortByKey()
            .map( p => (p._1._1,p._1._2,p._1._3,p._2))

            lineitemRDD.collect()
            .foreach{println}

            // lineitemRDD
            // .map(p => p._1+","+p._2+","+p._3+","+p._4)
            // .saveAsTextFile("Q5-Parquet.csv")

    }


  }
}
