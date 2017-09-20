package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ReduceForWords extends Reducer<IntWritable, Text, IntWritable,Text> {
    private String maxWord;
    private IntWritable max_length;
    private Set<String> few;
    private int single=1;
    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        maxWord = null;
        max_length=null;
        few=new HashSet<>();
    }
    public void reduce(IntWritable key, Iterable<Text> values,  Reducer<IntWritable, Text, IntWritable,Text>.Context con) throws IOException, InterruptedException {

        if(maxWord==null){
            maxWord=new String();
            String res;
            for (Text value:values){
                Text temp=new Text(value);
                few.add(temp.toString());
            }
            if(few.size()>single) {
                res = few.stream().collect(Collectors.joining(" , "));
            }
            else res=maxWord;
            con.write(new IntWritable(key.get()*-1),new Text(res));
        }

    }
}