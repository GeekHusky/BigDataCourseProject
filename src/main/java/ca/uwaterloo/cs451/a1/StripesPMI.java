package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;

import tl.lin.data.pair.PairOfFloatInt;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStFW;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;


public class StripesPMI extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  public static enum COUNTERS {
    LINE_COUNTER;
  }

  private static final class UniqueMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Map<String, Integer> uniqueToken = new HashMap<String, Integer>();

      // no more than 40 wordsc
      tokens = tokens.subList(0, Math.min(tokens.size(), 40));

      // get the unique words for the current line
      for (int i = 0; i < tokens.size(); i++) {
        if (uniqueToken.get(tokens.get(i)) == null) {
          uniqueToken.put(tokens.get(i), 1);
          KEY.set(tokens.get(i));
          context.write(KEY, ONE);
        }
      }

      // increment line number by 1
      context.getCounter(COUNTERS.LINE_COUNTER).increment(1);
    }
  }

  private static final class UniqueCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class UniqueReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class PMIMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {

    private static final Text KEY = new Text();
    private static final HMapStFW MAP = new HMapStFW();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      List<String> tokens = Tokenizer.tokenize(value.toString());

      Map<String, Integer> uniqueToken = new HashMap<String, Integer>();
      List<String> tokenList = new ArrayList<String>();

      // no more than 40 wordsc
      tokens = tokens.subList(0, Math.min(tokens.size(), 40));

      // get the unique words for the current line
      for (int i = 0; i < tokens.size(); i++) {
        if (uniqueToken.get(tokens.get(i)) == null) {
          uniqueToken.put(tokens.get(i), 1);
          tokenList.add(tokens.get(i));
        }
      }

      for (int i = 0; i < tokenList.size(); i++) {
        MAP.clear();
        for (int j = 0; j < tokenList.size(); j++) {
          if (i == j)
            continue;
          MAP.put(tokenList.get(j), 1);
        }

        KEY.set(tokenList.get(i));
        context.write(KEY, MAP);
      }
    }
  }

  private static final class PMICombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context) throws IOException, InterruptedException {

      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static final class PMIReducer extends Reducer<Text, HMapStFW, Text, Map<String, String>> {
    
    private static final Map<String, String> PMI = new HashMap<String, String>();

    private int threshold = 0;
    private float totalline = 0;
    private String sidedatapath;
    private Map<String, Float> word_line_count = new HashMap<String, Float>();

    @Override
    public void setup(Context context) throws IOException {
      threshold = context.getConfiguration().getInt("threshold", 0);
      totalline = (float)context.getConfiguration().getInt("totalline", 0);
      sidedatapath = context.getConfiguration().getStrings("sidedatapath")[0];

      Configuration conf = context.getConfiguration();
          
      try {      
        FileSystem fs = FileSystem.get(conf);

        // hadoop path - intermediate file
        Path side_data = new Path(sidedatapath);

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(side_data, true);
        BufferedReader reader=null;
        FSDataInputStream in=null;
        InputStreamReader inStream=null;

        while(fileStatusListIterator.hasNext()){
          LocatedFileStatus fileStatus = fileStatusListIterator.next();
      //do stuff with the file like ...
      
          side_data=fileStatus.getPath();

          if (!side_data.toString().substring(side_data.toString().lastIndexOf('/')+1).contains("part")) {continue;}

          reader = null;
          in = fs.open(side_data);
          inStream = new InputStreamReader(in);
          reader = new BufferedReader(inStream);
          String line = reader.readLine();

          
          while (line != null) {
    
              String[] parts = line.split("\\s+");
              if (parts.length != 2) {
                LOG.info("Wrong input format at line: '" + line + "'");
              } else {
                word_line_count.put(parts[0], Float.parseFloat(parts[1]));
              }
              line = reader.readLine();
            }

        }
        reader.close();
        in.close();
        inStream.close();
      } catch (IOException e) {
        throw new IOException("Something wrong with the File.");
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context) throws IOException, InterruptedException {

      PMI.clear();
      List<Float> PMIPair = new ArrayList<Float>();
      Map<String, Float> tempMap = new HashMap<String, Float>();

      for (HMapStFW hmap : values) {
        for (String key_val : hmap.keySet()) {
          if (tempMap.get(key_val) == null) {
            tempMap.put(key_val, hmap.get(key_val));
          } else {
            tempMap.put(key_val, hmap.get(key_val) + tempMap.get(key_val));
          }
        }
      }

      for (String key_val : tempMap.keySet()) {
        float cooccur_line = tempMap.get(key_val);

        if (cooccur_line >= threshold ) {
          float total_line = totalline;
          float a_line = word_line_count.get(key.toString());
          float b_line = word_line_count.get(key_val);
  
          float pmi_val = (float) Math.log10(cooccur_line * total_line / a_line / b_line);
  
          PMIPair.clear();
          PMIPair.add(pmi_val);
          PMIPair.add(cooccur_line);
          String temp=PMIPair.toString();
          PMI.put(key_val,temp);
          //PMI.put(key_val, PMIPair);
        }

      }

      context.write(key, PMI);

    }

  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {
  }

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 0;
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

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold of co-occurrence: " + args.threshold);

    // first mapreduce job
    Job job_unique = Job.getInstance(getConf());
    job_unique.setJobName("Unique");
    job_unique.setJarByClass(StripesPMI.class);

    job_unique.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_unique, new Path(args.input));
    FileOutputFormat.setOutputPath(job_unique, new Path(args.output + "-interm"));

    job_unique.setMapOutputKeyClass(Text.class);
    job_unique.setMapOutputValueClass(FloatWritable.class);
    job_unique.setOutputKeyClass(Text.class);
    job_unique.setOutputValueClass(FloatWritable.class);

    job_unique.setOutputFormatClass(TextOutputFormat.class);

    job_unique.setMapperClass(UniqueMapper.class);
    job_unique.setCombinerClass(UniqueCombiner.class);
    job_unique.setReducerClass(UniqueReducer.class);

    job_unique.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job_unique.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job_unique.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job_unique.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job_unique.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path intmediaPath = new Path(args.output + "-interm");
    FileSystem.get(getConf()).delete(intmediaPath, true);

    long startTime = System.currentTimeMillis();

    job_unique.waitForCompletion(true);

    // second mapreduce job
    Job job_pmi = Job.getInstance(getConf());
    job_pmi.setJobName("PMI");
    job_pmi.setJarByClass(StripesPMI.class);

    // Set the threshold for reducer
    job_pmi.getConfiguration().setInt("threshold", args.threshold);
    // Load the counter back to the second job
    job_pmi.getConfiguration().setInt("totalline",(int) job_unique.getCounters().findCounter(COUNTERS.LINE_COUNTER).getValue());
    // Load the path of intermediate file
    job_pmi.getConfiguration().setStrings("sidedatapath", args.output + "-interm");

    job_pmi.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_pmi, new Path(args.input));
    FileOutputFormat.setOutputPath(job_pmi, new Path(args.output));

    job_pmi.setMapOutputKeyClass(Text.class);
    job_pmi.setMapOutputValueClass(HMapStFW.class);
    job_pmi.setOutputKeyClass(Text.class);
    job_pmi.setOutputValueClass(Map.class);
    job_pmi.setOutputFormatClass(TextOutputFormat.class);

    job_pmi.setMapperClass(PMIMapper.class);
    job_pmi.setCombinerClass(PMICombiner.class);
    job_pmi.setReducerClass(PMIReducer.class);

    job_pmi.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job_pmi.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job_pmi.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job_pmi.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job_pmi.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job_pmi.waitForCompletion(true);
    FileSystem.get(getConf()).delete(intmediaPath, true);

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
    ToolRunner.run(new StripesPMI(), args);
  }

}
    