package org.seattlehadoop.ngram.mapreduce;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TokenCountShowMapper extends TableMapper<Text, IntWritable> {

	@Override
	protected void map(ImmutableBytesWritable key, org.apache.hadoop.hbase.client.Result value,
			org.apache.hadoop.mapreduce.Mapper<ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		String rowName = Bytes.toString(key.get(), key.getOffset(), key.getLength());
		for (Map.Entry<byte[], NavigableMap<Long, byte[]>> me : value.getMap().get(Constants.COLUMN_FAMILY).entrySet()) {
			String secondWord = Bytes.toString(me.getKey());
			int maxYear = 0;
			int maxCount = 0;
			for (Map.Entry<Long, byte[]> me2 : me.getValue().entrySet()) {
				int year = me2.getKey().intValue();
				int count = Bytes.toInt(me2.getValue());
				if (count > maxCount) {
					maxCount = count;
					maxYear = year;
				}
				if (rowName.compareTo(secondWord) < 0) {
					context.write(new Text("REV: " + secondWord + " " + rowName), new IntWritable(count));
				} else {
					context.write(new Text(rowName + " " + secondWord), new IntWritable(count));
				}
			}
		}

	};
}
