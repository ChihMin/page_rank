package calculateAverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CalculateAveragePartitioner extends Partitioner<Text, SumCountPair> {
	@Override
	public int getPartition(Text key, SumCountPair value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
        char chr = key.toString().charAt(0);
        if (chr >= 'a' && chr <= 'g')
            return 0;
        else
            return 1;

	}
}
