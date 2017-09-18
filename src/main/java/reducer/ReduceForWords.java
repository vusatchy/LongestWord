package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReduceForWords extends Reducer<IntWritable, Text, IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values,  Reducer<IntWritable, Text, IntWritable,Text>.Context con) throws IOException, InterruptedException {
        IntWritable max_length = new IntWritable(0);
        Text line = new Text("");
        Iterator<Text> itr = values.iterator();
        Text txt;
        while(itr.hasNext()){
            txt = new Text(itr.next());
            if(txt.getLength()>max_length.get()){
                max_length.set(txt.getLength());
                line = txt;
            }
        }

        con.write(max_length,line);
    }
}