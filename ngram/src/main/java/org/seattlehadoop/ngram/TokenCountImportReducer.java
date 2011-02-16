package org.seattlehadoop.ngram;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TokenCountImportReducer extends Reducer<Text, Text, Text, IntWritable> {

	private HTableInterface m_htable;
	public static byte[] COLUMN_FAMILY = Bytes.toBytes("details");

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, IntWritable>.Context context) throws java.io.IOException, InterruptedException {
		m_htable = new HTable(context.getConfiguration(), "tokens");
	};

	/**
	 * Only for unit testing
	 * 
	 * @param p_htable
	 */
	void setHtable(HTableInterface p_htable) {
		m_htable = p_htable;
	}

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, IntWritable>.Context context)
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
		m_htable.put(put);
	};

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, IntWritable>.Context context) throws java.io.IOException, InterruptedException {
		m_htable.close();
	};
}
