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

        Text pageKey = new Text();
        Text pageValue = new Text();
        if (pageTitle.compareTo("!chihmin_nodes") != 0) {
            String numOfEdgeStr = patterns[1];
            String nodeStr = pageStr.substring(
                pageTitle.length() + 1
            );
            pageKey.set(pageTitle);
            pageValue.set(nodeStr);
            
            int numOfEdge = Integer.valueOf(numOfEdgeStr);
            if (numOfEdge == 0) {
                Double alpha = 0.85;
                Double totalPages = Double.valueOf(numOfNodes);
                Double pageRank = alpha * Double.valueOf(patterns[patterns.length-1]) / totalPages;
                
                Text zeroKey = new Text();
                Text zeroValue = new Text();
                zeroKey.set("!chihmin_zero"); 
                zeroValue.set(String.valueOf(pageRank));
                
                //System.out.println("[Mapper] " + zeroValue.toString());
                context.write(zeroKey, zeroValue);
            }
            pageValue.set(nodeStr);
            context.write(pageKey, pageValue);
        } else {
            pageKey.set(pageTitle);
            pageValue.set(numOfNodes);
            context.write(pageKey, pageValue);    
        }
    }
}
