package calculateAverage;

import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalculateAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    private SumCountPair element ;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pageStr = value.toString();
        String[] patterns = pageStr.split("\t");
        String pageTitle = patterns[0];
        
        String lastPageRankStr = patterns[patterns.length - 3];
        String nowPageRankStr = patterns[patterns.length - 1];

        Double lastPageRank = Double.valueOf(lastPageRankStr);
        Double nowPageRank = Double.valueOf(nowPageRankStr);
        Double error = new Double(Math.abs(nowPageRank - lastPageRank));
        
        String newStr = pageStr.substring(
            pageTitle.length() + 1, 
            pageStr.length() - lastPageRankStr.length() - nowPageRankStr.length() - 4 
        ) + "\t" + nowPageRank;
        // System.out.println("[MAPPER " + pageTitle + "]->" + newStr);
        
        Text pageKey = new Text(pageTitle);
        Text pageValue = new Text(newStr);
        context.write(pageKey, pageValue);
        
        pageKey = new Text("!chihmin_nodes");
        pageValue = new Text("1");
        context.write(pageKey, pageValue);

        pageKey = new Text("!!!!chihmin_error");
        pageValue = new Text(String.valueOf(error));
        context.write(pageKey, pageValue);
    }
}
