package org.seattlehadoop.ngram.mapreduce.groupedtoken;

import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;

public class TokenWithSingleCountPair extends Pair<Utf8, Map<Utf8, Integer>> {
	private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
	private static final Schema INTEGER_MAP_SCHEMA = Schema.createMap(Schema.create(Type.INT));

	public TokenWithSingleCountPair(String input, int year, int count) {
		this(input, Collections.singletonMap(new Utf8(String.valueOf(year)), (Integer) count));
	}

	public TokenWithSingleCountPair(String input, Map<Utf8, Integer> p_output) {
		super(new Utf8(input), STRING_SCHEMA, p_output, INTEGER_MAP_SCHEMA);
	}

	public static Schema getMySchema() {
		return Pair.getPairSchema(STRING_SCHEMA, INTEGER_MAP_SCHEMA);
	}
}
