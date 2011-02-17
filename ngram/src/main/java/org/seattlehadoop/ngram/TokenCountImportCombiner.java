package org.seattlehadoop.ngram;

import static org.seattlehadoop.ngram.Constants.TAB;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Concats the values with tabs. So "a\t1" and "b\t2" becomes "a\t1\tb\t2"
 * 
 * @author cwilkes
 * 
 */
public class TokenCountImportCombiner extends Reducer<Text, Text, Text, Text> {

	public static String concatValues(Iterable<Text> values, String separator) {
		StringBuffer outvalue = new StringBuffer();
		for (Text t : values) {
			if (outvalue.length() > 0) {
				outvalue.append(separator);
			}
			outvalue.append(t.toString());
		}
		return outvalue.toString();
	}

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		context.write(key, new Text(concatValues(values, String.valueOf(TAB))));
	};
}
