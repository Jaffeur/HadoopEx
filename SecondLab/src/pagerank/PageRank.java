package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

/*
 * VERY IMPORTANT 
 * 
 * Each time you need to read/write a file, retrieve the directory path with conf.get 
 * The paths will change during the release tests, so be very carefully, never write the actual path "data/..." 
 * CORECT:
 * String initialVector = conf.get("initialRankVectorPath");
 * BufferedWriter output = new BufferedWriter(new FileWriter(initialVector + "/vector.txt"));
 * 
 * NOT CORRECT
 * BufferedWriter output = new BufferedWriter(new FileWriter(data/initialVector/vector.txt"));
 */

public class PageRank {
	
	public static void createInitialRankVector(String directory, long n) throws IOException 
	{
		File file = new File(directory);
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		
		for(int i=0 ; i< n ; i++){
			bw.write(i + " " + (1/((double)n)));
		}
		bw.close();
		fw.close();
	}
	
	public static boolean checkConvergence(String initialDir, String iterationDir, double epsilon) throws IOException
	{
		File file1 = new File(initialDir);
		File file2 = new File(iterationDir);
		FileReader fw1 = new FileReader(file1);
		FileReader fw2 = new FileReader(file2);
		BufferedReader br1 = new  BufferedReader(fw1);
		BufferedReader br2 = new BufferedReader(fw2);
			
		String line;
		double result = 0;
		while((line = br2.readLine()) != null){
			double val1 = Double.parseDouble(br2.readLine().split("\\s+")[1]);
			double val2 = Double.parseDouble(br1.readLine().split("\\s+")[1]);
			result += Math.abs(val1-val2);
		}
		br2.close();
		br1.close();
		fw2.close();
		fw1.close();
			
		if(result < epsilon) return true;
		else return false;
	}
	
	public static void avoidSpiderTraps(String vectorDir, long nNodes, double beta) throws IOException 
	{
		File file = new File(vectorDir);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		System.out.println("Après chargement file");
		
		String line = "";
		String input = "";
		while((line = br.readLine()) != null){
			String[] result = line.split("\\s+");
			String i = result[0];
			String j = result[1];
			double r = Double.parseDouble(result[2])*beta+((1-beta)/nNodes);
			input += i + " " + j + " " +r + "\n";
		}
		FileWriter fi = new FileWriter(file);
		fi.write(input);
		br.close();
		fr.close();
		fi.close();
		
	}
	
	public static void iterativePageRank(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException
	{
		// Directory parameters
				String initialVector = conf.get("initialRankVectorPath");
				String currentVector = conf.get("currentRankVectorPath");
				File cv = new File(currentVector);
				cv.createNewFile();
				String finalVector = conf.get("finalRankVectorPath"); 
				String intermediaryResultPath = conf.get("intermediaryResultPath");
				
				// to retrieve the number of nodes use long nNodes = conf.getLong("numNodes", 1); 
				
				// Remove deadends		
				conf.setLong("numNodes", 9);
				RemoveDeadends.job(conf);
				
				// GraphToMatrix
				GraphToMatrix.job(conf);		
				
				// Create initial vector
				createInitialRankVector(initialVector, conf.getLong("numNodes", 1));
				
				// Matrix vector multiplication, until convergence
				System.out.println("1");
				MatrixVectorMult.job(conf);
				System.out.println("2");
				long nNodes = conf.getLong("numNodes", 1);
				Double epsilon = conf.getDouble("epsilon", 0.1);
				System.out.println("3");
				Double beta = conf.getDouble("beta", 0.8);
				PageRank.avoidSpiderTraps(currentVector, nNodes, beta);
				System.out.println("4");
				Boolean testConvergence;
				testConvergence = PageRank.checkConvergence(initialVector, currentVector, epsilon);
				System.out.println("5");
				// Loop until convergence
				// Replace directory 'initialvector' by a copy of 'currentrankvector'
				while(!testConvergence) {
					FileUtils.deleteDirectory(new File (initialVector));	
					System.out.println("1");
					FileUtils.copyDirectory(new File (currentVector), new File (initialVector));	
					System.out.println("2");
					FileUtils.deleteDirectory(new File (currentVector));
					System.out.println("3");
					File oldfile = new File(initialVector + "/part-r-00000");
					System.out.println("4");
					oldfile.renameTo(new File(initialVector + "/vector.txt"));
					
					System.out.println("5");
					MatrixVectorMult.job(conf);		
					System.out.println("6");
					PageRank.avoidSpiderTraps(currentVector, nNodes, beta);
					testConvergence = PageRank.checkConvergence(initialVector, currentVector, epsilon);
					System.out.print(testConvergence);
				}
				FileUtils.copyDirectory(new File (currentVector), new File (finalVector));
	}
}
