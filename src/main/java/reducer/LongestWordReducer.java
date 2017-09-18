package reducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

public class LongestWordReducer extends Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
    {
        Iterator<Text> iterator=values.iterator();
        Text max =iterator.next();
        Text temp=null;
        while(iterator.hasNext()){
            temp=iterator.next();
            if(temp.getLength()>max.getLength()){
                max=temp;
            }
        }
        con.write(key,max);

    }
}


