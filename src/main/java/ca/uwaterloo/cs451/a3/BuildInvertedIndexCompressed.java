package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

import java.io.IOException;
import java.io.DataOutputStream;
import java.util.Iterator;
import java.util.List;

// 1. Index Compression. The index should be compressed 
// using VInts: see org.apache.hadoop.io.WritableUtils. 
// You should also use gap-compression (i.e., delta-compression) 
// techniques as appropriate.
// - gap-compression
// - Vint Compression

// 2. Buffering postings. The baseline indexer implementation 
// currently buffers and sorts postings in the reducer, which 
// as we discussed in class is not a scalable solution. Address 
// this scalability bottleneck using techniques we discussed in class and in the textbook.
// - use ((key, value), value) pair in Mapper and use secondary sorting
// - add partitionner to make sure everything goes to the right one

// 3. Term partitioning. The baseline indexer implementation 
// currently uses only one reducer and therefore all postings lists are 
// shuffled to the same node and written to HDFS in a single partition. 
// Change this so we can specify the number of reducers (hence, partitions) as a 
// command-line argument. This is, of course, easy to do, 
// but we need to make sure that the searcher understands this partitioning also.

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
 
    private static final PairOfStringInt KEY_PAIR = new PairOfStringInt();
    private static final IntWritable ONE = new IntWritable(1);
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit secondary sorting key-pairs
      for (PairOfObjectInt<String> e : COUNTS) {
        // term frequency for the word and the docid
        KEY_PAIR.set(e.getLeftElement(), (int) docno.get());
        context.write(KEY_PAIR, new IntWritable(e.getRightElement()));

        // doc frequency for the word
        KEY_PAIR.set(e.getLeftElement(),-1);
        context.write(KEY_PAIR, ONE);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {

    private static ByteArrayOutputStream BAOS = new ByteArrayOutputStream();
    private static final DataOutputStream DOS= new DataOutputStream(BAOS);
    private static String PREV ="";
    private static int PREV_DOC=0;

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      Iterator<IntWritable> iter = values.iterator();
      Text word = new Text();
      
      // emit the word
      if (!key.getLeftElement().equals(PREV) && !PREV.equals("") ) {
        word.set(PREV);
        DOS.flush();    
        context.write(word, new BytesWritable(BAOS.toByteArray()));
        BAOS.reset();
      }

      // count the current tf or df
      int tf=0;
      while (iter.hasNext()) {
        tf += iter.next().get();
      }  

      // if the term is with -1, then it's df for the word
      if (key.getRightElement() == -1) {
        PREV = key.getLeftElement();
        PREV_DOC=0;
        BAOS.reset();
        WritableUtils.writeVInt(DOS, tf);
      }
      // else the term is tf with corresponding docid
      else {
        // gap-compression + VInt-compression
        WritableUtils.writeVInt(DOS, (key.getRightElement()-PREV_DOC));
        WritableUtils.writeVInt(DOS, tf);

        PREV_DOC=key.getRightElement();
      }
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      
      // emit the last word and its df and tf
      Text word = new Text();
      DOS.flush();
      word.set(PREV);
      context.write(word, new BytesWritable(BAOS.toByteArray()));
      
      // reset all variables
      PREV="";
      PREV_DOC=0;
      BAOS.reset();
      DOS.close();
    }

  }

  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }



  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    // job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
