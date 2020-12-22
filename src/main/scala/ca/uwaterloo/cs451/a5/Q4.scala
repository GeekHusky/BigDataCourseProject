package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
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
            .map(line => (line.split('|')(0).toInt,line.split('|')(10)))
            .filter(p => p._2 == date_parameter)
            .cogroup(orders)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val red=p._2._1.size
                var final_pair : List[((Int,String),Int)] = List()
                for (i <- 1 to red) {
                    var nationID=customerBrod.value.get(p._2._2.head).get
                    var nationName=nationBrod.value.get(nationID).get
                    final_pair = final_pair :+ (((nationID,nationName),1))
                }
                final_pair
            })
            .reduceByKey(_+_)
            .sortByKey()
            .collect()
            .foreach{p => println("("+p._1._1+","+p._1._2+","+p._2+")")}

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
            .map(line => (line.getInt(0),line.getString(10)))
            .filter(p => p._2 == date_parameter)
            .cogroup(ordersRDD)
            .filter(p => p._2._1.size != 0)
            .flatMap(p => {
                val red=p._2._1.size
                var final_pair : List[((Int,String),Int)] = List()
                for (i <- 1 to red) {
                    var nationID=customerBrodRDD.value.get(p._2._2.head).get
                    var nationName=nationBrodRDD.value.get(nationID).get
                    final_pair = final_pair :+ (((nationID,nationName),1))
                }
                final_pair
            })
            .reduceByKey(_+_)
            .sortByKey()
            .collect()
            .foreach{p => println("("+p._1._1+","+p._1._2+","+p._2+")")}
    }


  }
}
