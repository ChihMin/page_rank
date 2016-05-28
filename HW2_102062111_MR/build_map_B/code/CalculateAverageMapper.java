package calculateAverage;

import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String pageStr = value.toString();
        String[] patterns = pageStr.split("\t");
        String title = patterns[0];
        
        if (title.compareTo("!chihmin_nodes") != 0) {
            for (int i = 1; i < patterns.length; ++i) {
                Text pageKey = new Text(patterns[i]);
                Text pageValue = new Text(title);
                context.write(pageKey, pageValue);
            }
            if (patterns.length == 1)
                context.write(new Text(title), new Text(""));
        } else {
            context.write(new Text(title), new Text(patterns[1]));
        }
    }
}
