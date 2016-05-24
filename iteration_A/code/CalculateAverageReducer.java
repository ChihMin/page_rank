package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text,Text,Text,Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String keyStr = key.toString();
        if (keyStr.compareTo("!chihmin_zero") == 0) {
            Double zeroDegree = new Double(0);
            for (Text val: values) {
                // System.out.println("[REDUCER] " + val.toString() + " " +  keyStr);
                Double pageRank = Double.valueOf(val.toString());
                zeroDegree = zeroDegree + pageRank;
            }
            Text value = new Text();
            value.set(String.valueOf(zeroDegree));
            context.write(key, value);
        } else {
            for (Text val: values)
                context.write(key, val);
        }
    }
}
