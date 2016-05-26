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
import org.apache.hadoop.conf.Configuration;

public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pageStr = value.toString();
        String[] patterns = pageStr.split("\t");
        String pageTitle = patterns[0];
        Configuration conf = context.getConfiguration();
        String numOfNodes = conf.get("N");
        
        if (pageTitle.compareTo("!chihmin_nodes") != 0) {
            Double pageRank = Double.valueOf(1)/Double.valueOf(numOfNodes);
            String nodeStr = pageStr.substring(pageTitle.length()+1);
            int numOfEdges = patterns.length - 1;

            Text pageKey = new Text();
            Text pageValue = new Text();
            pageKey.set(pageTitle);
            
            pageValue.set(String.valueOf(numOfEdges) + "\t" + nodeStr + "\t" + String.valueOf(pageRank));
            context.write(pageKey, pageValue); 
        } else {
            Text pageKey = new Text();
            Text pageValue = new Text();
            pageKey.set(pageTitle);
            pageValue.set(numOfNodes);
            context.write(pageKey, pageValue);    
        }
    }
}
