package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class PairsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class KeyPairsPartioner(numberOfPartitioner: Int) extends Partitioner {
  override def numPartitions: Int = numberOfPartitioner
  override def getPartition(key: Any): Int = key match {
     case (left, right) => (left.hashCode() & Integer.MAX_VALUE) % numberOfPartitioner
     case _ => 0
  }

}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf()
      .setAppName("Compute Bigram Relative Frequency Pairs")
      .set("spark.executor.memory", "1g")
      
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(),args.reducers())
     
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val pair_count = tokens.sliding(2).map(p => (p.head, p.last)).toList
          val word_count = tokens.init.map(w => (w, "*")).toList
          pair_count ++ word_count
        } else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new KeyPairsPartioner(args.reducers()))
      .mapPartitions(partition => {
        var marginal=0.0
        val res=partition.map(pair=>{
          if (pair._1._2 == "*"){
            marginal=pair._2
            (pair)
          } else {
            (pair._1, pair._2/marginal)
          }
        })
        res 
      })
      .map(p => "((" + p._1._1 + "," + p._1._2 + ")," + p._2+")")
      
    counts.saveAsTextFile(args.output())
  }
}
