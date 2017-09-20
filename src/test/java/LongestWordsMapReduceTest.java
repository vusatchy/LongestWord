import combiner.CombinerForWords;
import javafx.util.Pair;
import mapper.MapForWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.ReduceForWords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class LongestWordsMapReduceTest {

        MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
        ReduceDriver<IntWritable, Text, IntWritable,Text> reduceDriver;
        ReduceDriver<IntWritable, Text, IntWritable,Text> combineDriver;
        MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

        @Before
        public void setUp() {
            MapForWords mapper = new MapForWords();
            ReduceForWords reducer = new ReduceForWords();
            CombinerForWords combiner = new CombinerForWords();
            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
            combineDriver =  ReduceDriver.newReduceDriver(combiner);
        }

        @Test
        public void testCombiner(){
            final  String first="Yura";
            final  String second="name";
            List<Text> values = new ArrayList<Text>();
            values.add(new Text(first));
            values.add(new Text(second));
            combineDriver.withInput(new IntWritable(4), values);
            combineDriver.withOutput(new IntWritable(second.length()*-1), new Text(first));
            combineDriver.withOutput(new IntWritable(second.length()*-1), new Text(second));
            combineDriver.runTest();
        }

        @Test
        public void testMapper() {
            final String testword="testword";
            mapDriver.withInput(new LongWritable(0), new Text(
                    testword));
            mapDriver.withOutput(new IntWritable(testword.length()*-1), new Text(testword));
            mapDriver.runTest();
        }

        @Test
        public void testReducer() {
            final  String first="Yura";
            final  String second="name";
            List<Text> values = new ArrayList<Text>();
            values.add(new Text(first));
            values.add(new Text(second));
            reduceDriver.withInput(new IntWritable(4), values);
            reduceDriver.withOutput(new IntWritable(second.length()*-1), new Text(first+" , "+second));
            reduceDriver.runTest();
        }

    @Test
    public void testMapReduce() throws IOException {
        final  String first="Yura";
        final  String second="name";
        mapReduceDriver.withInput(new LongWritable(1), new Text(
                new String(first+" "+second)));
        mapReduceDriver.withOutput(new IntWritable(first.length()),new Text(first+" , "+second));
        mapReduceDriver.runTest();

    }
    }

