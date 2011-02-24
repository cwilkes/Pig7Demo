package org.seattlehadoop.ngram.mapreduce;

import static org.seattlehadoop.ngram.mapreduce.Constants.COLUMN_FAMILY;
import static org.seattlehadoop.ngram.mapreduce.TokenAndCount.fromTextSingle;
import static org.seattlehadoop.ngram.mapreduce.TokenAndCount.parseTokenAndCountTextValues;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TokenCountImportReducer extends TableReducer<Text, Text, Text> {

	private static final Text OUTPUT_KEY = new Text("out");

	void addSecondWordToPutRequest(Put put, String word, int count) {
		put.add(COLUMN_FAMILY, Bytes.toBytes(word), Bytes.toBytes(count));
	}

	Put createPutRequest(String word, int year) {
		return new Put(Bytes.toBytes(word), year);
	}

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Writable>.Context context)
			throws java.io.IOException, InterruptedException {
		TokenAndCount tokenKey = fromTextSingle(key);
		Put put = createPutRequest(tokenKey.getToken(), tokenKey.getCount());
		for (TokenAndCount tc : parseTokenAndCountTextValues(values)) {
			addSecondWordToPutRequest(put, tc.getToken(), tc.getCount());
		}
		context.write(OUTPUT_KEY, put);
	};

}
