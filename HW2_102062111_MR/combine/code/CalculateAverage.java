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
        BufferedReader br = new BufferedReader(
            new InputStreamReader(
                fs.open(new Path(args[0] + "/part-r-00000"))
            )
        ); 
        
        String line;
        Double error = null;
        while ((line = br.readLine()) != null) {
            String[] patterns = line.split("\t");
            String key = patterns[0];
            
            if (key.compareTo("!!!!chihmin_error") == 0) {
                error = Double.valueOf(patterns[1]);
                System.out.println(String.valueOf(error));
            }
        }


        br.close();
        		
		Job job = Job.getInstance(conf, "CalculateAverage");
		job.setJarByClass(CalculateAverage.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(CalculateAverageMapper.class);
		//job.setCombinerClass(CalculateAverageCombiner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		//job.setPartitionerClass(CalculateAveragePartitioner.class);
		job.setSortComparatorClass(CalculateAverageComparator.class);
		job.setReducerClass(CalculateAverageReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
        if (error.compareTo(0.001) < 0)
            job.waitForCompletion(true);
        System.exit(error.compareTo(0.001));
	}
}
