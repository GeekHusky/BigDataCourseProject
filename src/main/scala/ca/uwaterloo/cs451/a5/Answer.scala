package ca.uwaterloo.cs451.a5

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

object Answer {

  def main(argv: Array[String]) {

    val path="/data/cs451/TPC-H-10-PARQUET"
    // val path="TPC-H-0.1-PARQUET"
    
    val sparkSession=SparkSession.builder.getOrCreate
    var lineitem = sparkSession.read.parquet(path+"/lineitem")
    lineitem.registerTempTable("lineitem")
    var part = sparkSession.read.parquet(path+"/part")
    part.registerTempTable("part")
    var supplier = sparkSession.read.parquet(path+"/supplier")
    supplier.registerTempTable("supplier")
    var nation = sparkSession.read.parquet(path+"/nation")
    nation.registerTempTable("nation")
    var customer = sparkSession.read.parquet(path+"/customer")
    customer.registerTempTable("customer")
    var orders = sparkSession.read.parquet(path+"/orders")
    orders.registerTempTable("orders")

    // q1
    println("Q1: Answer")
    var query = "select count(*) from lineitem where l_shipdate = '1996-01-01'"
    var res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q2
    println("Q2: Answer")
    query = "select o_clerk, o_orderkey from lineitem, orders where l_orderkey = o_orderkey and l_shipdate = '1996-01-01' order by o_orderkey asc limit 20"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q3
    println("Q3: Answer")
    query = "select l_orderkey, p_name, s_name from lineitem, part, supplier where l_partkey = p_partkey and l_suppkey = s_suppkey and l_shipdate = '1996-01-01' order by l_orderkey asc limit 20"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q4
    println("Q4: Answer")
    query = "select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation where   l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and l_shipdate = '1996-01-01' group by n_nationkey, n_name order by n_nationkey asc"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q5
    println("Q5: Answer")
    query = "select n_nationkey, n_name, left(l_shipdate,7), count(*) from lineitem, orders, customer, nation where   l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and n_nationkey in (3,24) group by n_nationkey, n_name, left(l_shipdate,7) order by n_nationkey,left(l_shipdate,7) asc"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q6
    println("Q6: Answer")
    query = "select l_returnflag,l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice*(1-l_discount)) as sum_disc_price,sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_shipdate = '1996-01-01' group by l_returnflag, l_linestatus"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))

    // q7
    println("Q7: Answer")
    query = "select c_name, l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1996-01-01' and l_shipdate > '1996-01-01' group by c_name,l_orderkey,o_orderdate,o_shippriority order by revenue desc limit 10"
    res = sparkSession.sql(query)
    res.collect().foreach(r => println(r))


  }
}