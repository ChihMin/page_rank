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
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        
        String pageStr = value.toString();
        String[] patterns = pageStr.split("\t");
        String pageTitle = patterns[0];

        Configuration conf = context.getConfiguration();
        double N = Double.valueOf(conf.get("!chihmin_nodes"));
        double zeroDegree = Double.valueOf(conf.get("!chihmin_zero"));
        double alpha = 0.85;
        
        Text pageKey = new Text(pageTitle);
        if (fileName.compareTo("directed_map.txt") == 0) {
            if (pageTitle.compareTo("!chihmin_nodes") == 0) { 
                Text pageValue = new Text(conf.get("!chihmin_nodes"));
                context.write(pageKey, pageValue);
            } else if (pageTitle.compareTo("!chihmin_zero") != 0){
                String masterString = pageStr.substring(pageTitle.length() + 1) + "\t0";
                Text pageValue = new Text(masterString);
                context.write(pageKey, pageValue);
            }
        } else {
            String masterString = patterns[1] + "\t1";
            Text pageValue = new Text(masterString);
            context.write(pageKey, pageValue);
        }
    }
}
