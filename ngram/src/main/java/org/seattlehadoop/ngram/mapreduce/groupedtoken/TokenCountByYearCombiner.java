package org.seattlehadoop.ngram.mapreduce.groupedtoken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.mapred.Reporter;

public class TokenCountByYearCombiner extends AvroReducer<Utf8, Map<Utf8, Integer>, TokenWithSingleCountPair> {

	public static Map<Utf8, Integer> combine(Iterable<Map<Utf8, Integer>> p_values) {
		Map<Utf8, Integer> output = new HashMap<Utf8, Integer>();
		for (Map<Utf8, Integer> v : p_values) {
			// not sure if utf8 is imutable here
			for (Map.Entry<Utf8, Integer> me : v.entrySet()) {
				output.put(new Utf8(me.getKey().toString()), me.getValue());
			}
		}
		return output;
	}

	@Override
	public void reduce(Utf8 p_key, Iterable<Map<Utf8, Integer>> p_values, AvroCollector<TokenWithSingleCountPair> p_collector, Reporter p_reporter)
			throws IOException {
		p_collector.collect(new TokenWithSingleCountPair(p_key.toString(), combine(p_values)));
	}
}
