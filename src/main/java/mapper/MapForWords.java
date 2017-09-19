package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

public class MapForWords extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words = line.split("[-\\s,.()!।@?=¿'’*&%း▀   _+-;\"“”>|€$^]+");
        for (String word : words) {
            IntWritable outputKey = new IntWritable(1);
            Text outputValue = new Text(word.trim());
            con.write(outputKey, outputValue);
        }

    }

}
