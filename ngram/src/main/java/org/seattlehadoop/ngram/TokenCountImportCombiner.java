package org.seattlehadoop.ngram;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TokenCountImportCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, java.lang.Iterable<Text> arg1, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context arg2)
			throws java.io.IOException, InterruptedException {
		StringBuffer outvalue = new StringBuffer();
		for (Text t : arg1) {
			if (outvalue.length() > 0) {
				outvalue.append('\t');
			}
			outvalue.append(t.toString());
		}
		arg2.write(arg0, new Text(outvalue.toString()));
	};
}
