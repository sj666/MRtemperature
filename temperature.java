package mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
//
public class temperature
{
    public static class temMap extends Mapper<LongWritable,Text,Text,IntWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String year = value.toString().substring(0, 4);
            int temperaturem = Integer.parseInt(value.toString().substring(8));
            context.write(new Text(year), new IntWritable(temperaturem));

        }
    }
      public static class temReduce  extends Reducer<Text,IntWritable,Text,IntWritable> implements mapreduce.temReduce {

          @Override
          protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
              {
                  int maxValue=Integer.MIN_VALUE;
                  for (IntWritable value :values){
                      maxValue=Math.max(value.get(), maxValue);
                  }
                  context.write(key, new IntWritable(maxValue));
              }
          }
      }
      public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
          Configuration configuration=new Configuration();
          Job job=Job.getInstance(configuration);
          job.setJarByClass(temperature.class);

          job.setMapperClass(temMap.class);
          job.setReducerClass(temReduce.class);

          job.setInputFormatClass(TextInputFormat.class);
          FileInputFormat.addInputPath(job, new Path(args[0]));

          job.setOutputFormatClass(TextOutputFormat.class);
          FileOutputFormat.setOutputPath(job, new Path(args[1]));

          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(IntWritable.class);

          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
          job.waitForCompletion(true);
     }
    }



