package ca.uwaterloo.cs451.a5

import scala.reflect.io.Directory
import java.io.File

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date parameter", required = true)
  val text = opt[Boolean](descr = "use text", required = false)
  val parquet = opt[Boolean](descr = "use parquet", required = false)
  verify()
}


object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Use Text: " + args.text())
    log.info("User Parquet: "  + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date_parameter = args.date()
    val file_name_1="lineitem"
    
    if (args.text()) {

        val lineitem = sc.textFile(args.input()+"/"+file_name_1+".tbl")
            .filter(line => line.split('|')(10) == date_parameter)
            .map(line => {
                val fields=line.split('|')
                val returnflag=fields(8)
                val linestatus=fields(9)
                
                val quantity=fields(4).toDouble                
                val extendedprice=fields(5).toDouble            
                val discount=fields(6).toDouble
                val tax=fields(7).toDouble
                val discprice=extendedprice*(1-discount)
                val charge=discprice*(1+tax)

                ((returnflag,linestatus),(quantity,extendedprice,discprice,charge,1,discount))
            })
            .reduceByKey( (a,b) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5,a._6+b._6))
            .map(p => {
                val k=p._1
                val v=p._2
                (k._1,k._2,v._1,v._2,v._3,v._4,v._1/v._5,v._2/v._5,v._6/v._5,v._5)
            })
            .collect()
            .foreach{println}

    }
    else {

        val lineitemRDD = SparkSession.builder.getOrCreate
            .read.parquet(args.input()+"/"+file_name_1)
            .rdd
            .filter(line => line.getString(10) == date_parameter)
            .map(fields => {
                val returnflag=fields.getString(8)
                val linestatus=fields.getString(9)
                
                val quantity=fields.getDouble(4)
                val extendedprice=fields.getDouble(5)
                val discount=fields.getDouble(6)
                val tax=fields.getDouble(7)
                val discprice=extendedprice*(1-discount)
                val charge=discprice*(1+tax)

                ((returnflag,linestatus),(quantity,extendedprice,discprice,charge,1,discount))
            })
            .reduceByKey( (a,b) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5,a._6+b._6))
            .map(p => {
                val k=p._1
                val v=p._2
                (k._1,k._2,v._1,v._2,v._3,v._4,v._1/v._5,v._2/v._5,v._6/v._5,v._5)
            })
            .collect().foreach{println}

    }


  }
}
