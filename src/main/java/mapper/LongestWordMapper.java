package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class LongestWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        //String[] words = line.split("[-\\s,.()!।@?=¿'’*&%း▀   _+-;\"“”>|€$^]+");
        String[] words = line.split(" ");
        for(String word:words){
            Text outputValue = new Text(word);
            Text outputKey = new Text("key");
            con.write(outputKey, outputValue);
        }

       // String word= Arrays.asList(words).stream().max(String::compareTo).get();
       // System.out.println("map");
       // Text outputValue = new Text(word.trim());
        //IntWritable outputKey = new IntWritable(1);
        //con.write(outputKey, outputValue);
    }
}
