package calculateAverage;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text,Text,Text,Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int degree = 0;
        if (key.toString().compareTo("!chihmin_nodes") == 0) {
            for (Text val: values)
                context.write(key, val);
        } else {
            ArrayList<String> nextNodes = new ArrayList<String>();
            for (Text val: values) {
                String str = val.toString();
                if (str.length() != 0)
                    nextNodes.add(str);
            }
            context.write(key, new Text(String.join("\t", nextNodes)));
        }
	}
}
