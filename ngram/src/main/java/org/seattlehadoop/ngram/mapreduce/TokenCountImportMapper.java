package org.seattlehadoop.ngram.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.seattlehadoop.ngram.input.CountsArray;

import static org.seattlehadoop.ngram.mapreduce.Constants.*;

public class TokenCountImportMapper extends Mapper<Text, CountsArray, Text, Text> {

	@Override
	protected void map(Text key, CountsArray value, org.apache.hadoop.mapreduce.Mapper<Text, CountsArray, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		String[] firstAndSecondWord = key.toString().split("\\s+");
		if (firstAndSecondWord.length != 2) {
			context.getCounter("input", "error").increment(1);
			return;
		}
		String outKeyPrefix = firstAndSecondWord[0] + TAB;
		String outValuePrefix = firstAndSecondWord[1] + TAB;
		for (int[] yearAndVolume : value.getCounts()) {
			context.getCounter("year", String.valueOf(yearAndVolume[0])).increment(yearAndVolume[3]);
			context.write(new Text(outKeyPrefix + yearAndVolume[0]), new Text(outValuePrefix + yearAndVolume[3]));
		}
	};
}
