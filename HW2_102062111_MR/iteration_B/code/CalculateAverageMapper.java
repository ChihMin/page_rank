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
        Double N = Double.valueOf(conf.get("!chihmin_nodes"));
        Double zeroDegree = Double.valueOf(conf.get("!chihmin_zero"));

        if (pageTitle.compareTo("!chihmin_nodes") != 0 &&
            pageTitle.compareTo("!chihmin_zero") != 0 &&
            pageTitle.compareTo("!!!!chihmin_error") != 0) {
            
            String numOfEdgeStr = patterns[1];
            Double pageRank = Double.valueOf(patterns[patterns.length - 1]);
            Double alpha = new Double(0.85);
            // System.out.println("[MAPPER " + numOfEdgeStr + "] " + pageStr);
            int numOfEdge = Integer.valueOf(numOfEdgeStr);
            for (int i = 0; i < numOfEdge; ++i){
                int index = i + 2;
                String nextNode = patterns[index];
                // System.out.println("[" + pageTitle + "]->" + nextNode);
                if (nextNode.length() == 0)
                    continue;
                Text pageKey = new Text();
                Text pageValue = new Text();
                
                if (nextNode.length() > 0) {
                    char[] nextNodeArray = nextNode.toCharArray();
                    if (nextNodeArray[0] >= 'a' && nextNodeArray[0] <= 'z')
                        nextNodeArray[0] = Character.toUpperCase(nextNodeArray[0]);
                    nextNode = new String(nextNodeArray);
                }

                pageKey.set(nextNode);
                pageValue.set(String.valueOf(alpha * pageRank / Double.valueOf(numOfEdge)) + "\t1");

                context.write(pageKey, pageValue);
            }

            Text pageKey = new Text(pageTitle);
            Text pageValue = new Text(
                String.valueOf(
                    (Double.valueOf(1.0) - alpha) * 
                    (Double.valueOf(1.0) / N) + 
                    zeroDegree
                ) + "\t1"
            );
            context.write(pageKey, pageValue);

            pageKey = new Text(new String(pageTitle));
            pageValue = new Text(
                pageStr.substring(pageTitle.length() + 1) + "\t0"
            );
            
            context.write(pageKey, pageValue);
        }
    }
}
