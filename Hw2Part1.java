// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2015
import java.io.IOException;
import java.util.StringTokenizer;
import java.math.BigDecimal;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1{

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private DoubleWritable talk_time = new DoubleWritable();
    private Text source_dest = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      int i = 0; 
      StringBuilder source_destination =new StringBuilder("");
      StringTokenizer itr = new StringTokenizer(value.toString()," ");
      while (itr.hasMoreTokens()) {
        String nextToken = itr.nextToken();
        source_destination.append(nextToken+" ");
        if(i == 1)
        {
          source_dest.set(source_destination.toString());
        }
        else if(i == 2)
        {
          Double time = Double.parseDouble(nextToken);
          talk_time.set(time);  
        }             
        i++;       
      }
      context.write(source_dest,talk_time);
      System.out.println("Before Mapper: "+source_dest+"|"+talk_time);
    }
  }
  // This is the Reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  //
  // We want to control the output format to look at the following:
  //
  // count of word = count
  //
  public static class IntSumReducer
       extends Reducer<Text,DoubleWritable,Text,Text> {

    private Text count_time = new Text();
    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0.0;
      int count = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }

      double ave = (double) sum / count;
      String average = String.format("%.3f" , ave);
      System.out.println("After Mapper: "+key+"|"+sum+"|"+count+"|"+average);

      StringBuilder out_count_avg =new StringBuilder("");
      out_count_avg.append(Integer.toString(count)+" "+average.toString());
      count_time.set(out_count_avg.toString());

      context.write(key,count_time);
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
   
    Job job = Job.getInstance(conf, "Hw2Part1");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
     for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}