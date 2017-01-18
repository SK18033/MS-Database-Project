import java.io.*; /*Provides for system input and output through data streams, serialization and the file system */
import java.util.*; /* */ 
import java.net.*; /* */

import org.apache.hadoop.fs.*;    /* An abstract file system API in hadoop */
import org.apache.hadoop.conf.*;  /* Used for configuring system parameters */
import org.apache.hadoop.io.*;    /* Generic i/o code usage for reading/writing data to network, databases, files etc. */
import org.apache.hadoop.util.*;  /* Common utilities */
import org.apache.hadoop.mapred.*;/* An abstract base class for generic filesystem */


public class StrictTwoPhaseLocking{
/* MapReduce Application in Java for StrictTwoPhaseLocking (Strict 2PL) Concurrency Control Mechanism */

	static String team_id;
	static ArrayList<String> trans_ids = new ArrayList<String>();
	static String lock_status;
	static Integer S_count;
	static Integer X_count;
	
	static Map<String, ArrayList<String>> transIdMap = new HashMap<String,ArrayList<String>>();
	static Map<String, String> lockStatusMap = new HashMap<String, String>();
	static Map<String, Integer> sCountMap = new HashMap<String, Integer>();
	static Map<String, Integer> xCountMap = new HashMap<String, Integer>();
	
	public void populateMaps(){
		try{
			BufferedReader br = new BufferedReader(new FileReader("output.csv"));
			String line;
			
			while((line = br.readLine())!= null){
				String[] lineContent = line.split(",");
				team_id = lineContent[0];
				
				for(int i=1; i < lineContent.length; i++){
					trans_ids.add(lineContent[i]);
				}
				
				transIdMap.put(team_id, trans_ids);
				lockStatusMap.put(team_id, "unlocked");
				sCountMap.put(team_id,0);
				xCountMap.put(team_id,0);
			}
		}
		catch(IOException e){
			System.out.println("exception");
		}
	}
	
	
/**===========================================================================================================================
Mapper Class
===========================================================================================================================*/

	
	public static class StrictTwoPhaseLockingMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>, LogHistory {
		
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException, InterruptedException {

			String line = value.toString();
			String[] tokens = line.split("\t");
			String transid;
			String teamid;
			
			if(tokens != null){
				transid = tokens[0];
				teamid = tokens[1];
			}

			System.out.println("Transaction :" + "" + transid + " wants to read object/team " + teamid);
			
			String temp = readTeamData(transid,teamid);

			String team_value = transid;
			team_value.concat(temp);
			
			output.collect(new Text(teamid), new Text(team_value)); 
				
		}

		public String readTeamData(String trans_id, String team_id){


			if(lockStatusMap.get(team_id) == "unlocked")
			{
				System.out.println("Neither shared lock nor exclusive lock on object" + "" + team_id + "" + "is held by any transactions");
				lockStatusMap.replace(team_id, "S_lock");
				sCountMap.replace(team_id, 1);
				
				BufferedReader br1 = new BufferedReader(new FileReader("teams.csv"));
				String line;
				while((line = br1.readLine())!= null){
					if((line.indexOf(team_id)) != -1){
						return line;
					}
				}
				
			}

			else if(lockStatusMap.get(team_id) == "S_lock")
			{
				System.out.println("Shared lock on object" + "" + team_id + "is already held by other transactions");
				int scount = sCountMap.get(team_id);
				scount++;
				sCountMap.replace(team_id, scount);
				
				BufferedReader br1 = new BufferedReader(new FileReader("teams.csv"));
				String line;
				while((line = br1.readLine())!= null){
					if((line.indexOf(team_id)) != -1){
						return line;
					}
				}	
			}

			else
			{
				System.out.println("Exclusive lock on object" + "" + team_id + "is held, so no lock can be obtained");
				wait(10);
			}

		}
	}

/**===========================================================================================================================
 Reducer Class
 ===========================================================================================================================*/
	
	
	public static class StrictTwoPhaseLockingReducer extends MapReduceBase implements Reducer<Text, Text,Text, Text> {

		public void reduce(Text key, Text value,  OutputCollector<Text,Text> output, Reporter reporter) throws IOException, InterruptedException {

			String team_id = key;
			String transid = value.next().get(); 
			String[] team_value;
			int i = 0, min = 5, max = 20, res = 0;
			
			while (value.hasNext()) {
	            team_value[i] = value.next().get();
	            i++;
	        }
			
			if(lockStatusMap.get(team_id) == "unlocked")
			{
				System.out.println("Neither shared lock nor exclusive lock on object" + "" + team_id + "" + "is held by any transactions");
				lockStatusMap.replace(team_id, "X_lock");
				xCountMap.replace(team_id, 1);
			
				res = min + (int)(Math.random() * (max - min));
				team_value[(team_value.length) - 1 ] = String.valueOf(res); 
				
				lockStatusMap.replace(team_id, "unlocked");
				xCountMap.replace(team_id, 0);
				
				int scount = sCountMap.get(team_id);
				scount--;
				sCountMap.replace(team_id, scount);
				
				transIdMap.remove(team_id, transid);
				
				
			}

			else
			{
				System.out.println("Exclusive lock on object" + "" + team_id + "is held, so no lock can be obtained");
				wait(10);
			}
		
	        output.collect(key, new Text(team_value));
			
		}
	}
	
	
/**===========================================================================================================================
Main Function
===========================================================================================================================*/
			
	
	public static void main (String[] args) throws Exception
	{	/*Job configuration by setting different job-specific parameters */	

		
		StrictTwoPhaseLocking obj = new StrictTwoPhaseLocking();
		obj.populateMaps();
		
		JobConf jb_conf = new JobConf(StrictTwoPhaseLocking.class) /*primary user interface to describe a map-reduce job to Hadoop framework for execution */;
                
		jb_conf.setJobName("ConcurrencyControl");

		jb_conf.setInputFormat(KeyValueInputFormat.class); /* input specification for MapReduce job based on which input file is split */
		jb_conf.setOutputFormat(TextOutputFormat.class);   /* output specification for MapReduce job */

		jb_conf.setMapperClass(StrictTwoPhaseLockingMapper.class);
		jb_conf.setCombinerClass(StrictTwoPhaseLockingReducer.class);
		jb_Conf.setReducerClass(StrictTwoPhaseLockingReducer.class);

        jb_conf.setOutputKeyClass(Text.class);   /* sets expected output (key) type from both map and reduce phase */;
        jb_conf.setOutputValueClass(Text.class); /* sets expected output (value) type from both map and reduce phase */;
		
		/*Location of input and output files in distributed file system */
        FileInputFormat.setInputPaths(jb_conf, new Path());
		FileOutputFormat.setOutputPaths(jb_conf, new Path());

		JobClient.runJob(jb_conf); /*Submit/run a map/reduce job on Hadoop*/
	}
}
