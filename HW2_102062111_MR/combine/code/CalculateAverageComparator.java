package calculateAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CalculateAverageComparator extends WritableComparator {
	
	public CalculateAverageComparator() {
		super(Text.class, true);
	}
	
	public int compare(WritableComparable o1, WritableComparable o2) {
		Text key1 = (Text) o1;
		Text key2 = (Text) o2;
        
        String[] inputA = key1.toString().split("\t");
        String[] inputB = key2.toString().split("\t");

        Double a = Double.valueOf(inputA[1]);
        Double b = Double.valueOf(inputB[1]);
        String documentNameA = inputA[0];
        String documentNameB = inputB[0];
        if (a.compareTo(b) < 0)  return 1;
        if (a.compareTo(b) > 0)  return -1;
        else return documentNameA.compareTo(documentNameB);
	}
}
