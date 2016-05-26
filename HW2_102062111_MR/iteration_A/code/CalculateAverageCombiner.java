package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateAverageCombiner extends Reducer<Text,SumCountPair,Text,SumCountPair> {
	// Combiner implements method in Reducer
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
        int valueCount = 0;
		for (SumCountPair val: values) {
            sum += val.getSum();
            count += val.getCount();
            System.out.println(" ~~~~~~~ " + key + " -> " + String.valueOf(val.getSum()) + " " + String.valueOf(val.getCount()));
            valueCount++;
		}
        SumCountPair pair = new SumCountPair(sum, count);
        System.out.println("[COMBINER " + key + " " + String.valueOf(valueCount) + "]" + String.valueOf(pair.getSum()) + " " + String.valueOf(pair.getCount()));

        context.write(key, pair);
	}
}
