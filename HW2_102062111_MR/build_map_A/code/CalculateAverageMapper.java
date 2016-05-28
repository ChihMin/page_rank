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
        ArrayList<String> allMatches = new ArrayList<String>();
        //System.out.println("[MAPPER] " + pageStr);
        Matcher m = Pattern.compile("<title>(.+?)</title>").matcher(pageStr);
        if (m.find()) {
            String[] patterns = m.group().split("<title>|</title>");
            String title = replaceChar(patterns[1]);
            ArrayList<String> nextNodes = getNextNodeList(pageStr);
            
            Text pageKey = new Text(title);
            Text pageValue = new Text("0");
            context.write(pageKey, pageValue);
            for (String nextNode: nextNodes) {
                Text nextKey = new Text(
                    nextNode.substring(0, 1).toUpperCase() +
                    nextNode.substring(1)    
                );
                Text nextValue = new Text("1\t" + title);
                context.write(nextKey, nextValue);
            }

            /* Below is calculate outdegree 0 pages */
            Text degree = new Text();
            Text degKey = new Text();
            String degStr = "!chihmin_nodes";
            degKey.set(degStr);
            degree.set(String.valueOf(1));
            context.write(degKey, degree);
        }
    }
    
    private String replaceChar(String str) {
        str = str.replaceAll("&quot;", "\""); 
        str = str.replaceAll("&gt;", ">");
        str = str.replaceAll("&lt;", "<");
        str = str.replaceAll("&amp;", "&");
        str = str.replaceAll("&apos;", "'");
        return str;
    }

    private ArrayList<String> getNextNodeList(String str) {
        ArrayList<String> allMatches = new ArrayList<String>();
        Matcher m = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])").matcher(str);
        while (m.find()) {
            String[] patterns = m.group().split("[\\[\\]\\|#]+");
            if (patterns.length > 1) 
                allMatches.add(replaceChar(patterns[1]));
        }
        return allMatches;
    }
}
