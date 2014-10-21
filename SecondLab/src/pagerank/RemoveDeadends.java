package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RemoveDeadends {

	enum myCounters{ 
		NUMNODES;
	}

	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] values = value.toString().split("\\s+");
			context.write(new Text(values[0]), new Text("s "+values[1]));  
			context.write(new Text(values[1]), new Text("p "+values[0])); 
		}
	}
	

	static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<String> sucessors = new ArrayList<String>();
			ArrayList<String> predecessors = new ArrayList<String>();
			
			for ( Text i: values){
				String[] result = i.toString().split("\\s+");
				if (result[0].equals("s")) sucessors.add(result[1]);
				else predecessors.add(result[1]);
			}
			
			if( sucessors.size() != 0){ //si à des successeurs
				Counter c = context.getCounter(myCounters.NUMNODES);
				c.increment(1);
					for (String i : predecessors){
							if(!i.equals(key.toString())){
								context.write(new Text(i),key); //alors pour tous les predecesseurs on revoit key -> predecesseurs
							}
					}					
				}
			}
		 
	}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		
		boolean existDeadends = true;
		
		//you don't need to use or create other folders besides the two listed below
		String intermediaryDir = conf.get("intermediaryResultPath");
		String currentInput = conf.get("processedGraphPath");
		
		FileUtils.copyDirectory(new File(conf.get("graphPath")), new File(conf.get("processedGraphPath")));
		long nNodes = conf.getLong("numNodes", 0);

		
		while(existDeadends){
			
			Job job = Job.getInstance(conf);
			job.setJobName("deadends job");
			
			job.setMapOutputKeyClass(Text.class); //on défini l'output du mappeur
			job.setMapOutputValueClass(Text.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(TextInputFormat.class); //format d'entrée
			job.setOutputFormatClass(TextOutputFormat.class); //format de sortie

			FileInputFormat.setInputPaths(job, new Path(currentInput));  
			FileOutputFormat.setOutputPath(job, new Path(intermediaryDir));
			
			job.waitForCompletion(true);
			
			FileUtils.deleteQuietly(new File(currentInput));
			FileUtils.copyDirectory(new File(intermediaryDir), new File(currentInput));
			FileUtils.deleteQuietly(new File(intermediaryDir));
			//update number of nodes
			
			Counters counters = job.getCounters();
			Counter c = counters.findCounter(myCounters.NUMNODES);
			if(c.getValue() == nNodes){
				existDeadends = false;
			}else{
				nNodes = c.getValue();
				conf.setLong("numNodes", nNodes);
			}
			
		}		
		
		
		
	}
	
}
