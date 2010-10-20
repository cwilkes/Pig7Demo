package com.ladro.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import a.b.MyKey;
import c.d.MyValue;

public class CreateLuceneMapper extends Mapper<LongWritable, Text, MyKey, MyValue> {
	@Override
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws java.io.IOException, InterruptedException {
	};

}
