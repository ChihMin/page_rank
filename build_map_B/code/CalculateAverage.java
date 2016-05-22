package calculateAverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;


public class CalculateAverage {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(args[0]));
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                fs.open(new Path("HW2/map_input/part-r-00000"))
            )
        ); 
        
        String line;
        int number = 0; 
        while ((line = br.readLine()) != null) {
            String[] patterns = line.split("\t");
            String title = patterns[0];
            String replaceStr = title + "\t";
            String nextNodes = line.substring(title.length() + 1);
            //System.out.println(line);
            if (title.compareTo("!chihmin_nodes") == 0) {
                System.out.println(line);
                String[] keyValuePair = line.split("\t");
                conf.set("N", keyValuePair[1]);
                break;
            }

            number = number + 1;
            if (number % 1000000 == 1)
                System.out.println(String.valueOf(number));  
        }
 
        		
		Job job = Job.getInstance(conf, "CalculateAverage");
		job.setJarByClass(CalculateAverage.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(CalculateAverageMapper.class);
		//job.setCombinerClass(CalculateAverageCombiner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		//job.setSortComparatorClass(xxx.class);
		//job.setReducerClass(CalculateAverageReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		// job.setNumReduceTasks(2);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
