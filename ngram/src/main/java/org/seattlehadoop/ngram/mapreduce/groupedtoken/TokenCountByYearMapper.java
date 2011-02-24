package org.seattlehadoop.ngram.mapreduce.groupedtoken;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.mapred.Reporter;
import org.seattlehadoop.ngram.avro.TokenCount;

public class TokenCountByYearMapper extends AvroMapper<TokenCount, TokenWithSingleCountPair> {

	@Override
	public void map(TokenCount p_datum, AvroCollector<TokenWithSingleCountPair> p_collector, Reporter p_reporter) throws IOException {
		p_collector.collect(new TokenWithSingleCountPair(p_datum.token.toString(), p_datum.year, p_datum.volumeCount));
	}
}
