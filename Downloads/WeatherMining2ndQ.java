import java.io.IOException;
import java.text.DateFormatSymbols;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMining2ndQ {

    // this is the name of the jar you compile, so hadoop has its fucntion hooks
    private final static String JAR_NAME = "wm2.jar";


    public static class TokenizerMapper
    extends Mapper < Object, Text, Text, IntWritable > {

        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            String data = value.toString();
            int year = Integer.parseInt(data.substring(15, 19));
            int month = Integer.parseInt(data.substring(19, 21));
            int temp;
            String monthL = new DateFormatSymbols().getMonths()[month - 1];
            String res = "";
            if (data.charAt(87) == '+') {
                temp = Integer.parseInt(data.substring(88, 92));
            } else {
                temp = Integer.parseInt(data.substring(87, 92));
            }
            res = "YEAR " + year + " MONTH " + monthL;
            context.write(new Text(res), new IntWritable(temp));
            res = "YEAR " + year;
            context.write(new Text(res), new IntWritable(temp));
        }
    }

    public static class IntSumReducer
    extends Reducer < Text, IntWritable, Text, Text > {

        public void reduce(Text key, Iterable < IntWritable > values,
            Context context
        ) throws IOException,
        InterruptedException {

            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            float count = 0;
            float sum = 0;
            for (IntWritable i: values) {
                float temp = i.get();
                min = Math.min(min, temp);
                max = Math.max(max, temp);
                count += 1;
                sum += temp;
            }
            float avg = sum / count;
            String res = "Min: " + min + " MAX: " + max + " AVG: " + avg;
            context.write(new Text(key), new Text(res));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather mining");
        job.setJar(JAR_NAME);
        job.setJarByClass(WeatherMining2ndQ.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
