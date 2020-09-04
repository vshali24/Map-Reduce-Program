import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    
    private Text word = new Text();

    public void map(Object key, Text value, Context context1
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int count = itr.countTokens();
      count=count-2; // to ignore first value which is the key and : 
      System.out.println(count); 
      String k = itr.nextToken();
      String waste = itr.nextToken();
      word.set(k);
      while (itr.hasMoreTokens()) {
        Integer num = Integer.parseInt(itr.nextToken()); // taking value as integer
        IntWritable one = new IntWritable(num);
        if(count<=10){
          context1.write(word, one);
        }
      }
    }
  }


  public static class IntSumReducer
  extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values,
                  Context context1
                  ) throws IOException, InterruptedException {
 int sum = 0;
 int count = 0;
 for (IntWritable val : values) {
   sum += val.get();
   count++;
 }
 result.set(sum);
 System.out.println(count);
  System.out.println(key);
  System.out.println(result);
  
 context1.write(key, result);
}
}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}