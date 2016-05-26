package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text, Text, Text, Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().compareTo("!chihmin_nodes") == 0) {
            int number = 0;
            for (Text val: values) {
                number = number + Integer.valueOf(val.toString());
            }
            Text value = new Text(String.valueOf(number));
            context.write(key, value);
        } else if (key.toString().compareTo("!!!!chihmin_error") == 0){
            Double errorSum = new Double(0);
            for (Text val: values) {
                Double error = Double.valueOf(val.toString());
                errorSum = errorSum + error;
            }
            Text value = new Text(String.valueOf(errorSum));
            context.write(key, value);
        } else {
            for (Text val: values)
                context.write(key, val);
        }
    }
}
