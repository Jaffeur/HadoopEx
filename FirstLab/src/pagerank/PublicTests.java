package pagerank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

/**
 * This class contains the public test cases for the project. The public tests
 * are distributed with the project description, and students can run the public
 * tests themselves against their own implementation.
 * 
 * Any changes to this file will be ignored when testing your project.
 * 
 */
public class PublicTests extends BaseTests {
	static {
		String inputVector = "data/initialVector";
		String inputMatrix = "data/initialMatrix";
		String output = "data/currentVector";
		String intermediary = "data/intermediaryDirectory";
		try {
			FileUtils.deleteQuietly(new File(output));
			FileUtils.deleteQuietly(new File(intermediary));
			Configuration conf = new Configuration();
			//MatrixVectorMultiplication
			conf.set("inputVectorPath", inputVector);
			conf.set("inputMatrixPath", inputMatrix);
			conf.set("intermediaryResultPath", intermediary);
			conf.set("currentVectorPath", output);
			FileUtils.deleteQuietly(new File("data/currentVector"));
			MatrixVectorMult.multiplicationJob(conf);
			readOutputVector(output);

			//GraphToMatrix Job
			conf.set("graphPath", "data/graph");
			conf.set("stochasticMatrixPath", "data/stochasticMatrix");
			
			FileUtils.deleteQuietly(new File("data/stochasticMatrix"));
			GraphToMatrix.GraphToMatrixJob(conf);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			initializationError = e.toString();
		}
	}
	
	public void testFirstMapper() throws IOException{
		MatrixVectorMult.FirstMap m = new MatrixVectorMult.FirstMap();
		MapDriver<LongWritable, Text, IntWritable, Text> mapDriver = MapDriver.newMapDriver(m);
		mapDriver.withInput(new LongWritable(0),new Text("1 1 0.5"));
		mapDriver.withOutput(new IntWritable(1), new Text("1 0.5"));
		mapDriver.runTest();
	}
	
	public void testSecondMapper() throws IOException{
		MatrixVectorMult.SecondMap m = new MatrixVectorMult.SecondMap();
		MapDriver<Text, Text, Text, Text> mapDriver = MapDriver.newMapDriver(m);
		mapDriver.withInput(new Text("1"),new Text("0.1"));
		mapDriver.withOutput(new Text("1"), new Text("0.1"));
		mapDriver.runTest();
	}
	
	public void testCombiner() throws IOException {
		MatrixVectorMult.CombinerForSecondMap c = new MatrixVectorMult.CombinerForSecondMap();
		ReduceDriver<Text, Text, Text, Text> combinerDriver = ReduceDriver.newReduceDriver(c);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0.5")) ;
		values.add(new Text("0.1"));
		
		combinerDriver.withInput(new Text("1"), values);
		combinerDriver.withOutput(new Text("1"), new Text("0.6"));
		combinerDriver.runTest();
	}
	
	
	public void testSecondReducer() throws IOException {
		MatrixVectorMult.SecondReduce r = new MatrixVectorMult.SecondReduce();
		ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver = ReduceDriver.newReduceDriver(r);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0.25"));
		values.add(new Text("0.25"));
		values.add(new Text("0.25"));
		values.add(new Text("0.25"));
		reduceDriver.withInput(new Text("2"), values);
		reduceDriver.withOutput(new Text("2"), new DoubleWritable(1.0));
		reduceDriver.runTest();
	}

	public void testVectorSize() {
		assertEquals(4, outputVector.size());
	}
	
	public void testVectorSumofElements()
	{
		double sum = 0.;
		for(int line:outputVector.keySet())
		{
			sum += outputVector.get(line);
		}
		assertEquals(1.0, sum, 0.1);
	}
	
	public void testConstructionStochasticMatrix(){
		try {

			File directory = new File("data/stochasticMatrix");
			if(!directory.exists())
				fail("Output directory  doesn't exist");
			
			File[] contents = directory.listFiles();
			File outputFile = null;
		
			for (int i = 0; i < contents.length; ++i)
				if (!contents[i].getName().equals("_SUCCESS")
						&& !contents[i].getName().startsWith("."))
					outputFile = contents[i].getAbsoluteFile();
		
			if (outputFile == null)
				fail("Output file  doesn't exist");
			
			HashMap<Integer, Double> sumColumns = new HashMap<Integer, Double>();
			BufferedReader r = new BufferedReader(new FileReader(outputFile));
			
			String line;
			while ((line = r.readLine()) != null) {
				String[] parts = line.split("\\s+");
				if(parts.length < 3)
					continue;
				
				if(sumColumns.get(Integer.valueOf(parts[1])) == null)
					sumColumns.put(Integer.valueOf(parts[1]), Double.valueOf((parts[2])));
				else
					sumColumns.put(Integer.valueOf(parts[1]), sumColumns.get(Integer.valueOf(parts[1])) + Double.valueOf(parts[2]));
			}
		  
			r.close();
			if(sumColumns.isEmpty())
				fail("no output");
			for(int k:sumColumns.keySet())
				if(Math.abs(sumColumns.get(k) - 1) > 0.01)
					fail("Matrix is not stochastic");
			
		} catch (IOException e) {
			System.out.println(e.toString());
			fail(e.toString());
		}
		}
}

