package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class CalculateAverageReducer extends Reducer<Text,Text,Text,Text> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().compareTo("!chihmin_nodes") == 0) {
            int degree = 0;
            for (Text val: values) {
                degree += Integer.valueOf(val.toString());
            }
            String degStr = String.valueOf(degree);
            Text result = new Text();
            result.set(degStr);
            context.write(key, result);
        } else {
            boolean isLegalNode = false; 
            ArrayList<String> nextNodes = new ArrayList<String>();
            for (Text val: values) {
                String[] patterns = val.toString().split("\t");
                if (patterns[0].compareTo("0") == 0) {
                    // Here is master
                    String str = new String(key.toString());
                    context.write(new Text(str), new Text(""));
                    isLegalNode = true;
                } else {
                    nextNodes.add(patterns[1]);
                }
            }
            if (isLegalNode) {
                Text value = new Text(String.join("\t", nextNodes));
                context.write(key, value);
            }
        }
	}
}
