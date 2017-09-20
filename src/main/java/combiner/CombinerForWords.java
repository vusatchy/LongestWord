package combiner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

    public class CombinerForWords extends Reducer<IntWritable, Text, IntWritable,Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,  Reducer<IntWritable, Text, IntWritable,Text>.Context con) throws IOException, InterruptedException {
            IntWritable max_length = new IntWritable(0);
            Text line = new Text("");
            Iterator<Text> itr = values.iterator();
            Text txt;
            List<String> few = new ArrayList<>();
            while(itr.hasNext()){
                txt = new Text(itr.next());
                if(txt.getLength()>max_length.get()){
                    max_length.set(txt.getLength());
                    line = txt;
                    few.clear();

                }
                if(txt.getLength()==max_length.get()){
                    few.add(txt.toString());
                }
            }
            for (String value :few
                 ) {
                con.write(new IntWritable(value.length()*-1),new Text(value));
            }

    }

}
