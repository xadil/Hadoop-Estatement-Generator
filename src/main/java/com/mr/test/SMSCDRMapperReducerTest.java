package com.mr.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ab.statements.mapred.StatementsMapper;
import com.ab.statements.mapred.StatementsReducer;
 
public class SMSCDRMapperReducerTest {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Iterable<Text>> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Iterable<Text>> mapReduceDriver;
 
  @Before
  public void setUp() {
	  StatementsMapper mapper = new StatementsMapper();
	  StatementsReducer reducer = new StatementsReducer();
//	  mapDriver = MapDriver.newMapDriver(mapper);
//	  reduceDriver = ReduceDriver.newReduceDriver(reducer);
//	  mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(), new Text(
        "1000000001|0000000001|DR|18000.00|MI000001|ROTANA HOTELS|31-12-2016 00:14|REF00000001"));
    mapDriver.withOutput(new Text("1000000001"), new Text("1000000001|0000000001|DR|18000.00|MI000001|ROTANA HOTELS|31-12-2016 00:14|REF00000001"));
    mapDriver.runTest();
  }
 
  /*@Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("6"), values);
    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
    reduceDriver.runTest();
  }
   
  @Test
  public void testMapReduce() {
    mapReduceDriver.withInput(new LongWritable(), new Text(
              "655209;1;796764372490213;804422938115889;6"));
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
    mapReduceDriver.runTest();
  }*/
}
