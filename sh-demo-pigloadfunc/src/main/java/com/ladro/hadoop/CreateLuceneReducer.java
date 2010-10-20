package com.ladro.hadoop;

import org.apache.hadoop.mapreduce.Reducer;
import a.b.MyKey;
import c.d.MyValue;

public class CreateLuceneReducer extends Reducer<MyKey, MyValue, MyKey, MyValue> {
	@Override
	protected void reduce(MyKey key, java.lang.Iterable<MyValue> values, org.apache.hadoop.mapreduce.Reducer<MyKey, MyValue, MyKey, MyValue>.Context context)
			throws java.io.IOException, InterruptedException {
	};

}
