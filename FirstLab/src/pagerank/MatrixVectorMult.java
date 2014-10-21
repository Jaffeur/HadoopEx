package pagerank;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.objenesis.instantiator.basic.NewInstanceInstantiator;


public class MatrixVectorMult {
	
	
	static class FirstMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] values = value.toString().split("\\s+");

				if( values.length == 3){ 	//matrice A
					IntWritable cle = new IntWritable(Integer.parseInt(values[1].toString()));
					Text text = new Text(values[0] + " " + values[2] );
					context.write(cle, text);
				}else if (values.length == 2){ 						//vecteur v
					int j = Integer.parseInt(values[0].toString());
					IntWritable cle = new IntWritable(j);
					Text text = new Text(values[1].toString());
					context.write(cle, text);
				}
			}
		}
	

	static class FirstReduce extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
		private DoubleWritable partialResult = new DoubleWritable ();
		
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<Integer,Double> matrixColumn = new HashMap<Integer,Double> ();
			
			double vj = 0;
			
			for(Text value : values){
				String[] data = value.toString().split("\\s+");
				
				if(data.length == 2){ //matrice
					matrixColumn.put(Integer.parseInt(data[0].toString()), Double.parseDouble(data[1].toString()));
				}else if(data.length == 1){
					vj = Double.parseDouble(data[0].toString());
				}
			}
			
			for(int i : matrixColumn.keySet() ){
				context.write(new IntWritable(i), new DoubleWritable(matrixColumn.get(i)*vj));
			}
			
			
		}
	}
	

	static class SecondMap extends Mapper<Text, Text, Text, Text> {
		
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException 
		{				
			context.write(key, value);		
		}
	}
	

	static class CombinerForSecondMap extends Reducer<Text, Text, Text, Text> {
		private Text partialResult = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			for(Text value : values){
				sum += Double.parseDouble(value.toString());
			}
			context.write(key, new Text(Double.toString(sum)));
		}
	}

	static class SecondReduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable finalResult = new DoubleWritable ();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double sum = 0;
			for(Text value : values){
				sum += Double.parseDouble(value.toString());
			}
			context.write(key, new DoubleWritable(sum));
			
			//throw new UnsupportedOperationException("Implementation missing");	
		}
	}
	
	
	public static void multiplicationJob(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		// First job
		Job job1 = Job.getInstance(conf);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setMapperClass(FirstMap.class);
		job1.setReducerClass(FirstReduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path[]{new Path(conf.get("inputVectorPath")), new Path(conf.get("inputMatrixPath"))});
		FileOutputFormat.setOutputPath(job1, new Path(conf.get("intermediaryResultPath")));

		job1.waitForCompletion(true);
		
		// Second job
		Job job2 = Job.getInstance(conf);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);
		
		// If your implementation of the combiner passed the unit test, uncomment the following line
		// job2.setCombinerClass(CombinerForSecondMap.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(conf.get("intermediaryResultPath")));
		FileOutputFormat.setOutputPath(job2, new Path(conf.get("currentVectorPath")));

		job2.waitForCompletion(true);
	}
	
}

