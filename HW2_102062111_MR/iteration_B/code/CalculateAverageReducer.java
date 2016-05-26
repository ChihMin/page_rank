package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text, Text, Text, Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double sum = new Double(0);
        int number = 0;
        String master = null;
        for (Text value: values) {
            String valueStr = value.toString();
            String[] patterns = valueStr.split("\t");
            String isMaster = patterns[patterns.length - 1];
/*
            if (key.toString().compareTo("Roman Catholic Church") == 0) {
                System.out.println("[MAPPER " + key.toString() + "]->" + value.toString());
            }
*/
            if (isMaster.compareTo("0") == 0) {
                master = valueStr; 
            } else {
                sum = sum + Double.valueOf(patterns[0]);
                number = number + 1;
            }
        }
        
        if (master != null) {
            Text value = new Text();
            value.set(master + "\t" + String.valueOf(sum));
            // System.out.println("[MAPPER " + key.toString() + "]->" + value.toString());
            context.write(key, value); 
        }
    }
}
