import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherMining1stQ {
    private final static String JAR_NAME = "wm1.jar";   

    public static class TokenizerMapper
    extends Mapper < Object, Text, NullWritable, Text > {

        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {

            String data = value.toString();
            int temp;
            if (data.charAt(87) == '+') {
                temp = Integer.parseInt(data.substring(88, 92));
            } else {
                temp = Integer.parseInt(data.substring(87, 92));
            }
            if ((temp) > 30) {
                context.write(NullWritable.get(), new Text(data));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather mining");
        job.setJar(JAR_NAME);
        job.setJarByClass(WeatherMining1stQ.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
