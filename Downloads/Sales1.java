import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DateFormatSymbols;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sales1 {

  // this is the name of the jar you compile, so hadoop has its fucntion hooks
  private final static String JAR_NAME = "sa1.jar";   

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, FloatWritable>{

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String data[] = value.toString().split("\\t");
		  context.write(new Text(data[2]+"___"+data[3]), new FloatWritable(Float.valueOf(data[4])));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> it,
                       Context context
                       ) throws IOException, InterruptedException {
      String s = key.toString();
		  float d = 0;
		  for (FloatWritable i:it) {
			  d+=i.get();
		  }
		  context.write(new Text(s+"___TOTAL"), new FloatWritable(d));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weather mining");
    job.setJar( JAR_NAME );
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setJarByClass(Sales1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

