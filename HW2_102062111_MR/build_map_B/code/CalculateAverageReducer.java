package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageReducer extends Reducer<Text,SumCountPair,Text,DoubleWritable> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
        
        for (SumCountPair val: values) {
            sum += val.getSum();
            count += val.getCount();
		}
        // SumCountPair pair = new SumCountPair(sum, count);
        System.out.println("[REDUCER] " + String.valueOf(sum) + " " + String.valueOf(count));
		double dSum = Double.valueOf(sum);
        double dCount = Double.valueOf(count);
        DoubleWritable result = new DoubleWritable(dSum / dCount);
        context.write(key, result);
	}
}
