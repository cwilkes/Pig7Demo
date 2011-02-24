package org.seattlehadoop.ngram.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.seattlehadoop.ngram.avro.TokenCount;
import org.seattlehadoop.ngram.mapreduce.groupedtoken.TokenCountByYearCombiner;

public class TokenCountByYearCombinerTest {

	@SuppressWarnings({ "deprecation", "unchecked" })
	@Test
	public void testCombine() throws IOException {
		JobConf job = new JobConf();
		AvroJob.setInputSchema(job, TokenCount.SCHEMA$);
		AvroJob.setReducerClass(job, TokenCountByYearCombiner.class);
		Reducer reducer = ReflectionUtils.newInstance(job.getReducerClass(), job);		
		ReduceDriver<AvroKey<String>, AvroValue<Map<Integer, Integer>>, AvroKey<String>, AvroValue<Map<Integer, Integer>>> reduceDriver = new ReduceDriver<AvroKey<String>, AvroValue<Map<Integer, Integer>>, AvroKey<String>, AvroValue<Map<Integer, Integer>>>(
				reducer);
		List<AvroValue<Map<Integer, Integer>>> values = new ArrayList<AvroValue<Map<Integer, Integer>>>();
		values.add(new AvroValue<Map<Integer,Integer>>(Collections.singletonMap(1910, 2)));
		values.add(new AvroValue<Map<Integer,Integer>>(Collections.singletonMap(1911, 3)));
		reduceDriver.setInput(new AvroKey<String>("aaa bbb"), values);
		reduceDriver.run();
	}
}
