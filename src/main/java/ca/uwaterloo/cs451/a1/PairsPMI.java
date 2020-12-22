package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfFloats;
import tl.lin.data.pair.PairOfInts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import tl.lin.data.map.HMapStIW;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.List;

import javax.print.attribute.HashAttributeSet;

public class PairsPMI extends Configured implements Tool {

    // counters
    public static enum COUNTERS{
        LINE_COUNTER;
    }
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);


    // UniqueMapper class which is the first mapper for processing the file and count
    // number of lines containing each words in the file
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
      
      // 
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



    private static final class PMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);
  
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
                for (int j = 0; j < tokenList.size(); j++) {
                if (i == j) continue;
                PAIR.set(tokenList.get(i), tokenList.get(j));
                context.write(PAIR, ONE);
              }
            }


        }
    }

    private static final class PMICombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();
        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
              sum += iter.next().get();
            }
      
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class PMIReducer extends Reducer <PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
        private static final PairOfFloatInt PMI = new PairOfFloatInt();
        
        private int threshold = 0;
        private float totalline = 0f;
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
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Iterator<IntWritable> iter = values.iterator();
            int cooccur_line = 0;

            while (iter.hasNext()) {
              cooccur_line += iter.next().get();
            }        


            if ( cooccur_line >= threshold ) {

                float a_line = word_line_count.get(key.getLeftElement());
                float b_line = word_line_count.get(key.getRightElement());
                float pmi_val = (float)Math.log10((float)cooccur_line * totalline / a_line / b_line);
      
                PMI.set(pmi_val,cooccur_line);
                context.write(key, PMI);
                
            }
        }
    }

    private static final class PMIPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
          return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }


  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

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

    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold of co-occurrence: " + args.threshold);

    // first mapreduce job
    Job job_unique = Job.getInstance(getConf());
    job_unique.setJobName("Unique");
    job_unique.setJarByClass(PairsPMI.class);
    job_unique.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_unique, new Path(args.input));
    FileOutputFormat.setOutputPath(job_unique, new Path(args.output+"-interm"));

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
    Path intmediaPath = new Path(args.output+"-interm");
    FileSystem.get(getConf()).delete(intmediaPath, true);

    // start the time tracker
    long startTime = System.currentTimeMillis();
  
    job_unique.waitForCompletion(true);

    // second mapreduce job
    Job job_pmi = Job.getInstance(getConf());
    job_pmi.setJobName("PMI");
    job_pmi.setJarByClass(PairsPMI.class);

    // Set the threshold for reducer
    job_pmi.getConfiguration().setInt("threshold", args.threshold);
    // Load the counter back to the second job
    job_pmi.getConfiguration().setInt("totalline",(int) job_unique.getCounters().findCounter(COUNTERS.LINE_COUNTER).getValue());
    // Load the path of intermediate file
    job_pmi.getConfiguration().setStrings("sidedatapath", args.output + "-interm");

    job_pmi.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job_pmi, new Path(args.input));
    FileOutputFormat.setOutputPath(job_pmi, new Path(args.output));

    job_pmi.setMapOutputKeyClass(PairOfStrings.class);
    job_pmi.setMapOutputValueClass(IntWritable.class);
    job_pmi.setOutputKeyClass(PairOfStrings.class);
    job_pmi.setOutputValueClass(PairOfFloatInt.class);
    job_pmi.setOutputFormatClass(TextOutputFormat.class);

    job_pmi.setMapperClass(PMIMapper.class);
    job_pmi.setCombinerClass(PMICombiner.class);
    job_pmi.setReducerClass(PMIReducer.class);
    job_pmi.setPartitionerClass(PMIPartitioner.class);

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
    ToolRunner.run(new PairsPMI(), args);
  }

}
    