# BigDataCourseProject

## Build Project

```
mvn clean package
```

## Running Command

a0 

WordCount: counting the number of words in a document through Hadoop 
PerfectX: counting the frequency of the word "perfect"
```
$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a0.WordCount \
   -input data/Shakespeare.txt -output wc

$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a0.PerfectX \
   -input data/Shakespeare.txt -output cs451-bigdatateach-a0-shakespeare
```

a1

calculating the [Pointwise Multual Information](http://en.wikipedia.org/wiki/Pointwise_mutual_information) (PMI) through Pairs and Stripes imeplementation in Hadoop.

With pairs, each co-occurring word pair is stored separately; with stripes, all words co-occurring with a conditioning word are stored together in an associative array.

```
$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a1.PairsPMI \
   -input data/Shakespeare.txt -output cs451-bigdatateach-a1-shakespeare-pairs \
   -reducers 5 -threshold 10

$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a1.StripesPMI \
   -input data/Shakespeare.txt -output cs451-bigdatateach-a1-shakespeare-stripes \
   -reducers 5 -threshold 10

```

a2

calculating the [Bigram Relative Frequencey](https://en.wikipedia.org/wiki/Bigram) and [Pointwise Multual Information](http://en.wikipedia.org/wiki/Pointwise_mutual_information) (PMI) through Pairs and Stripes imeplementation in Spark.

```
$ spark-submit --class ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyPairs \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output cs451-lintool-a2-shakespeare-bigrams-pairs --reducers 5

$ spark-submit --class ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyStripes \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output cs451-lintool-a2-shakespeare-bigrams-stripes --reducers 5

$ spark-submit --class ca.uwaterloo.cs451.a2.PairsPMI \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output cs451-lintool-a2-shakespeare-pmi-pairs --reducers 5 --threshold 10

$ spark-submit --class ca.uwaterloo.cs451.a2.StripesPMI \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output cs451-lintool-a2-shakespeare-pmi-stripes --reducers 5 --threshold 10
```

a3

Simulate and build the index for set of documents for the search engine. Using the technology like:
- Index Compression with VInts.

- Use [Secondary Sorting](https://medium.com/@sudarshan_sreenivasan/what-is-secondary-sort-in-hadoop-and-how-does-it-work-fe35609b5319) to deal with
Buffering postings (buffering the postings might cause memory issues) issues

- Term partioning to has the ability for partitionly running the program
```
$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a3.BuildInvertedIndexCompressed \
   -input data/Shakespeare.txt -output cs451-bigdatateach-a3-index-shakespeare -reducers 4

$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a3.BooleanRetrievalCompressed \
   -index cs451-bigdatateach-a3-index-shakespeare -collection data/Shakespeare.txt \
   -query "outrageous fortune AND"

$ hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a3.BooleanRetrievalCompressed \
   -index cs451-bigdatateach-a3-index-shakespeare -collection data/Shakespeare.txt \
   -query "white red OR rose AND pluck AND"
```

a5 

Implemented hand-crafting Spark program to do SQL Data Analytics.
Corresponding SQL code is in Answers.scala

a6

Training a stochastic gradient descent in Spark by replicating the work by [Efficient and Effective Spam Filtering and Re-ranking for Large Web Datasets](http://arxiv.org/abs/1004.5168)

a7

Implement the spark streaming technology.