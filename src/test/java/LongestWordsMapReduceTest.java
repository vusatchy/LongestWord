import javafx.util.Pair;
import mapper.MapForWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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


public class LongestWordsMapReduceTest {

        MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
        ReduceDriver<IntWritable, Text, IntWritable,Text> reduceDriver;
        MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

        @Before
        public void setUp() {
            MapForWords mapper = new MapForWords();
            ReduceForWords reducer = new ReduceForWords();
            mapDriver = MapDriver.newMapDriver(mapper);
            reduceDriver = ReduceDriver.newReduceDriver(reducer);
            mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        }

        @Test
        public void testMapper() {
            mapDriver.withInput(new LongWritable(0), new Text(
                    "testword"));
            mapDriver.withOutput(new IntWritable(1), new Text("testword"));
            mapDriver.runTest();
        }

        @Test
        public void testReducer() {
            final  String smaller="name";
            final  String longer="someverylongword";
            List<Text> values = new ArrayList<Text>();
            values.add(new Text(smaller));
            values.add(new Text(longer));
            reduceDriver.withInput(new IntWritable(1), values);
            reduceDriver.withOutput(new IntWritable(longer.length()), new Text(longer));
            reduceDriver.runTest();
        }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "kafka spark zookeeper"));
        mapReduceDriver.withOutput(new IntWritable("zookeeper".length()),new Text("zookeeper"));
        mapReduceDriver.runTest();
    }
    }

