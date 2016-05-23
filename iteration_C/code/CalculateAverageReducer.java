package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text, Text, Text, Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int number = 0;
        for (Text val: values) {
            number++;
            context.write(key, val);
        }
        System.out.println("[REDUCER " + key.toString() + "] " + String.valueOf(number)); 
    }
}
