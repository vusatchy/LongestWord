
import mapper.MapForWords;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.ReduceForWords;


public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        {
            Configuration c = new Configuration();
            String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
            Path input = new Path(args[0]);
            Path output = new Path(args[1]);
            Job j = new Job(c, "LongestWord");
            j.setJarByClass(Main.class);
            j.setMapperClass(MapForWords.class);
            j.setReducerClass(ReduceForWords.class);
            j.setOutputKeyClass(IntWritable.class);
            j.setOutputValueClass(Text.class);
            j.setNumReduceTasks(1);
            FileInputFormat.addInputPath(j, input);
            FileOutputFormat.setOutputPath(j, output);
            System.exit(j.waitForCompletion(true) ? 0 : 1);
        }
    }




    }
