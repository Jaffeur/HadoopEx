package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GraphToMatrix {

	static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] values = value.toString().split("\\s+");
				context.write(new IntWritable(Integer.parseInt(values[1])), new IntWritable(Integer.parseInt(values[0])));
			}
		}
	
	static class Reduce extends Reducer<IntWritable, IntWritable, NullWritable, Text> {
			
			protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
				
				ArrayList<IntWritable> array = new ArrayList<IntWritable>();
				int count = 0;
				
				for(IntWritable value : values ){
					count ++;
					array.add(value);
				}
				
				for( int i = 0 ; i< array.size() ; i++ ){
					float f = 1/((float) count);
					Text text = new Text( array.get(i).toString() + " " + key.toString() + " " + f);
					context.write(null, text);
				}
			}
	 	} 
		
	
	public static void GraphToMatrixJob(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(conf.get("graphPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("stochasticMatrixPath")));
		job.waitForCompletion(true);
	}
}
