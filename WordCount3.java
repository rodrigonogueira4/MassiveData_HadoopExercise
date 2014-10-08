import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount3 {
        
 public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splited = value.toString().split("\\t+");
		if (splited[0].length() == 7) {
			context.write(new IntWritable(Integer.parseInt(splited[1])), new Text(splited[0]));
		}
    }
 } 
        
 public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {

    private final List<String[]> orderedlist = new ArrayList<String[]>();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        String[] keyvalue  = new String[2];
        for (Text val : values) {
			keyvalue[0] = val.toString();
		}		
		keyvalue[1] = key.toString();
		orderedlist.add(keyvalue);
    }

	@Override
	protected void cleanup(Context context) throws IOException,
	  InterruptedException {
	  for (int i=orderedlist.size()-1; i>=orderedlist.size()-100; i--) {
	    context.write(new Text(orderedlist.get(i)[0]), new IntWritable(Integer.parseInt(orderedlist.get(i)[1])));
	  }
	}

 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
