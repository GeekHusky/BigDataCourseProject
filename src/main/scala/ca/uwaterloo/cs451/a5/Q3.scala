package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
    val file_name_1="lineitem"
    val file_name_2="part"
    val file_name_3="supplier"

    
    if (args.text()) {
        val part = sc.textFile(args.input()+"/"+file_name_2+".tbl")
            .map(line => (line.split('|')(0),line.split('|')(1)))
            .collectAsMap()
        val supplier = sc.textFile(args.input()+"/"+file_name_3+".tbl")
            .map(line => (line.split('|')(0),line.split('|')(1)))
            .collectAsMap()
        val partBrod = sc.broadcast(part)
        val supplierBrod = sc.broadcast(supplier)

        val lineitem = sc.textFile(args.input()+"/"+file_name_1+".tbl")
            .filter(line => line.split('|')(10) == date_parameter)
            .map(line => {
                var fields=line.split('|')
                var orderkey=fields(0).toInt
                var partKey=fields(1)
                var suppKey=fields(2)

                var partName=partBrod.value.get(partKey).get
                var suppName=supplierBrod.value.get(suppKey).get

                (orderkey,(partName,suppName))
            })
            .sortByKey()
            .take(20)
            .foreach{p => println("("+p._1+","+p._2._1+","+p._2._2+")")}
    }
    else {

         val partRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_2)
            .rdd
            .map(line => (line.getInt(0),line.getString(1)))
            .collectAsMap()
        val supplierRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_3)
            .rdd
            .map(line => (line.getInt(0),line.getString(1)))
            .collectAsMap()

        val partBrodRDD = sc.broadcast(partRDD)
        val supplierBrodRDD = sc.broadcast(supplierRDD)

        val lineitemRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_1)
            .rdd
            .filter(line => line.getString(10) == date_parameter)
            .map(fields => {
                var orderkey=fields.getInt(0)
                var partKey=fields.getInt(1)
                var suppKey=fields.getInt(2)

                var partName=partBrodRDD.value.get(partKey).get
                var suppName=supplierBrodRDD.value.get(suppKey).get

                (orderkey,(partName,suppName))
            })
            .sortByKey()
            .take(20)
            .foreach{p => println("("+p._1+","+p._2._1+","+p._2._2+")")}
    }


  }
}
