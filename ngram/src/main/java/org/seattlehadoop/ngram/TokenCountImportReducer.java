package org.seattlehadoop.ngram;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TokenCountImportReducer extends TableReducer<Text, Text, Text> {

	public static byte[] COLUMN_FAMILY = Bytes.toBytes("details");
	private static final Text OUTPUT_KEY = new Text("out");

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Writable>.Context context)
			throws java.io.IOException, InterruptedException {
		String[] keyParts = key.toString().split("\\t");
		String firstWord = keyParts[0];
		int year = Integer.parseInt(keyParts[1]);
		Put put = new Put(Bytes.toBytes(firstWord), year);
		for (Text v : values) {
			String[] secondWordAndCount = v.toString().split("\\t");
			int i = 0;
			while (i < secondWordAndCount.length) {
				String secondWord = secondWordAndCount[i++];
				int count = Integer.parseInt(secondWordAndCount[i++]);
				put.add(COLUMN_FAMILY, Bytes.toBytes(secondWord), Bytes.toBytes(count));
			}
		}
		context.write(OUTPUT_KEY, put);
	};

}
