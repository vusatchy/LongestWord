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
    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
        maxWord = new String();
        max_length=new IntWritable(0);
        few=new HashSet<>();
    }
    public void reduce(IntWritable key, Iterable<Text> values,  Reducer<IntWritable, Text, IntWritable,Text>.Context con) throws IOException, InterruptedException {

        Iterator<Text> itr = values.iterator();
        Text txt;
        if(key.get()>=max_length.get()) {
            while (itr.hasNext()) {
                txt = new Text(itr.next());
                if (txt.getLength() > max_length.get()) {
                    max_length.set(key.get());
                    maxWord = txt.toString();
                    few.clear();

                }
                if (txt.getLength() == max_length.get()) {
                    few.add(txt.toString());
                }
            }
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String res=few.stream().collect(Collectors.joining(" , "));
        context.write(new IntWritable(maxWord.length()),new Text(res));
    }
}