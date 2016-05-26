package calculateAverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CalculateAveragePartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
        String keyStr = key.toString();
        char chr = keyStr.charAt(0);
        if (chr == '!') return 0;
        else {
            int hashCode = keyStr.hashCode();
            return hashCode % 3;
        }
	}
}
