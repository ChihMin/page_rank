package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text,Text,Text,Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int degree = 0;
        if (key.toString().compareTo("!chihmin_nodes") == 0) {
            for (Text val: values) {
                degree += Integer.valueOf(val.toString());
            }
            String degStr = String.valueOf(degree);
            Text result = new Text();
            result.set(degStr);
            context.write(key, result);
        } else {
            for (Text val: values)
                context.write(key, val);
        }
	}
}
