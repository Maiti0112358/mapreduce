
/******************************************************************************************
 * File: 		 WordCount.java
 * Author:		 Eeran Maiti
 * Date Created: 14 June 2014
 * Change Log:	 Eeran Maiti
 * Description:	 Implementation of Map Reduce that runs another jar file. No of mappers set by input file.
 * Instructions: Set the variables sJarFile, sParam1, sParam2
 * 				 Follow this command to run it
 * 				 hadoop jar Wordcount.jar <input location> <output location> <jar file> <parameter1> <parameter2>
 * Reference:	 Apache MapReduce tutorial (http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html)
 *****************************************************************************************/

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import com.google.common.primitives.Ints;

import java.util.ArrayList;

public class WordCount extends Configured implements Tool {
	
	public static List<String> lDatFile = new ArrayList<String>();
	static Log log = LogFactory.getLog(WordCount.class);
	public static String sDocFormat = "pdf";
	public static String sParam1 = "/data/IP";
	public static String sParam2 = "/data/OP";
	public static String sJarFile = "/data/programs/jar_File.jar";

	
	/******************************************************************************************
	 * Class : Map
	 * Description: Implements the mapper.
	 *****************************************************************************************/	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;
		private String sProgDirectory = "/root/Documents/programs";
     
		/******************************************************************************************
		 * Method : configure
		 * Return type: void
		 * @param: JobConf job
		 * Description: Takes input file and configures.
		 *****************************************************************************************/		
		public void configure(JobConf job) {
			
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");
	
			if (job.getBoolean("wordcount.skip.patterns", false)) {
		        Path[] patternsFiles = new Path[0];
		        try {
		        	patternsFiles = DistributedCache.getLocalCacheFiles(job);
		        } catch (IOException ioe) {
		        	System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
		        }
		        for (Path patternsFile : patternsFiles) {
		        	parseSkipFile(patternsFile);
		        }
			}
		}

		/******************************************************************************************
		 * Method : parseSkipFile
		 * Return type: void
		 * @param: Path patternsFile
		 * Description: Decides pattern to skip.
		 *****************************************************************************************/		
		private void parseSkipFile(Path patternsFile) {
			
			try {
					BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
					String pattern = null;
					while ((pattern = fis.readLine()) != null) {
						patternsToSkip.add(pattern);
					}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		/******************************************************************************************
		 * Constructor : map
		 * Return type: void
		 * @param: LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter
		 * Description: mapping.
		 *****************************************************************************************/				
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

			// The following method runs external jar for every mapper
			runJar();
       
			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}
			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
     
		/******************************************************************************************
		 * Method : runJar
		 * Return type: void
		 * @param: void
		 * Description: Runs a jar file from inside mapper.
		 *****************************************************************************************/				
		public void runJar( ) {
			
			//Build command 
			List<String> commands = new ArrayList<String>();			

			//Create the command
			commands.add("java");
			commands.add("-Djava.awt.headless=false");
			commands.add("-jar");
			commands.add(sJarFile);
			commands.add(sParam1);
			commands.add(sParam2);

			//Display final command
			System.out.println(commands);

			//Run process on target
			ProcessBuilder pb = new ProcessBuilder(commands);
         	pb.directory(new File(sProgDirectory));
			pb.redirectErrorStream(true);
			Process process = null;
			try {
				process = pb.start();
			} catch (Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}
			
			//Read output
			StringBuilder out = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = null, previous = null;
			try {
				while ((line = br.readLine()) != null)
					if (!line.equals(previous)) {
						previous = line;
						out.append(line).append('\n');
						System.out.println(line);
					}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
				e.printStackTrace();
			}

			//Check result
			try {
				if (process.waitFor() == 0)
					System.out.println("Mapper successful.");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
				e.printStackTrace();
			}

		}
	}

	/******************************************************************************************
	 * Class : Reduce
	 * Description: Implements the reducer.
	 *****************************************************************************************/		
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		/******************************************************************************************
		 * Constructor : reduce
		 * Return type: void
		 * @param: Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter
		 * Description: reducing.
		 *****************************************************************************************/						
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	/******************************************************************************************
	 * Method : run
	 * Return type: int
	 * @param: String[] args
	 * Description: Sets up and runs the whole map reduce program.
	 *****************************************************************************************/				
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i=0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("wordcount.skip.patterns", true);
			} else {
				other_args.add(args[i]);
			}
		}
		
		//Set extra parameters for the jar file
		sJarFile= other_args.get(2);
		sParam1 = Long.parseLong(other_args.get(3));
		sParam2 = Integer.parseInt(other_args.get(4));																			
		
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		JobClient.runJob(conf);
		return 0;
	}


	/******************************************************************************************
	 * Method : main
	 * Return type: void
	 * @param: String String[] args
	 * Description: Entry point of program.
	 *****************************************************************************************/							
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
}
//End of class WordCount.