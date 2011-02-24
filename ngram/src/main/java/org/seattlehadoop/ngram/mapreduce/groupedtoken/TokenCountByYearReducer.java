package org.seattlehadoop.ngram.mapreduce.groupedtoken;

import static org.seattlehadoop.ngram.mapreduce.groupedtoken.TokenCountByYearCombiner.combine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.Reporter;
import org.seattlehadoop.ngram.avro.GroupedToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenCountByYearReducer extends AvroReducer<Utf8, Map<Utf8, Integer>, GroupedToken> {
	private final Logger m_logger = LoggerFactory.getLogger(getClass());

	@Override
	public void reduce(Utf8 p_key, Iterable<Map<Utf8, Integer>> p_values, AvroCollector<GroupedToken> p_collector, Reporter p_reporter) throws IOException {
		GroupedToken groupedToken = new GroupedToken();
		String[] parts = p_key.toString().split("\\s+", 2);
		if (parts.length != 2) {
			m_logger.warn("Input '" + p_key + "' did not parse into two parts: " + Arrays.toString(parts));
			p_reporter.getCounter("output", "error").increment(1);
			return;
		}
		groupedToken.firstPhrase = new Utf8(parts[0]);
		groupedToken.secondPhrase = new Utf8(parts[1]);
		groupedToken.byYear = combine(p_values);
		p_collector.collect(groupedToken);
	}
}
