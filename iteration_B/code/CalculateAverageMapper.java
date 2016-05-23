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

public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pageStr = value.toString();
        String[] patterns = pageStr.split("\t");
        String pageTitle = patterns[0];

        Configuration conf = context.getConfiguration();
        double N = Double.valueOf(conf.get("!chihmin_nodes"));
        double zeroDegree = Double.valueOf(conf.get("!chihmin_zero"));
        

        if (pageTitle.compareTo("!chihmin_nodes") != 0 &&
                pageTitle.compareTo("!chihmin_zero") != 0) {
            
            String numOfEdgeStr = patterns[1];
            double pageRank = Double.valueOf(patterns[patterns.length - 1]);
            double alpha = 0.85;
            // System.out.println("[MAPPER " + numOfEdgeStr + "] " + pageStr);
            int numOfEdge = Integer.valueOf(numOfEdgeStr);
            for (int i = 0; i < numOfEdge; ++i){
                int index = i + 2;
                String nextNode = patterns[index];
                // System.out.println("[" + pageTitle + "]->" + nextNode);
                if (nextNode.length() == 0)
                    continue;
                Text pageKey = new Text();
                DoubleWritable pageValue = new DoubleWritable();
                 
                pageKey.set(nextNode);
                pageValue.set(alpha * pageRank / Double.valueOf(numOfEdge));

                context.write(pageKey, pageValue);
            }

            Text pageKey = new Text(pageTitle);
            DoubleWritable pageValue = new DoubleWritable(
                (1 - alpha) * (1 / N) + zeroDegree
            );
            context.write(pageKey, pageValue);
        }
    }
}
