import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class StopWords {

  // this is the name of the jar you compile, so hadoop has its fucntion hooks
  private final static String JAR_NAME = "sw.jar";   

  public static class TokenizerMapper
       extends Mapper<Object, Text, NullWritable, Text>{

    private final static IntWritable one = new IntWritable();
    private Text word = new Text();

    HashSet<String> sett = new HashSet<String>();
    static int count =0;
    public void setup(Context context) throws IOException{
      Path pt=new Path(context.getConfiguration().get("stopwords_file"));//Location of file in HDFS
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;
      line=br.readLine();
      while (line != null){
          StringTokenizer itr = new StringTokenizer(line);
          while (itr.hasMoreTokens() ) {
            sett.add(itr.nextToken().toLowerCase());
          }
          System.out.println(line);
          line=br.readLine();
      }
  }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String str ="";
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens() ) {
        String temp = itr.nextToken();
        if(!(sett.contains(temp.toLowerCase()))) {
          str = str + " " + temp;
        }
      }
      word.set(str);
      context.write(NullWritable.get(), word);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("stopwords_file", args[1]);
    Job job = Job.getInstance(conf, "stop word");
    job.setJar( JAR_NAME );
    job.setJarByClass(StopWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

