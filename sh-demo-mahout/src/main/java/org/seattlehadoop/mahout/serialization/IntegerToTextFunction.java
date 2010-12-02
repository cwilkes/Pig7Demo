package org.seattlehadoop.mahout.serialization;

import org.apache.hadoop.io.Text;

import com.google.common.base.Function;

public class IntegerToTextFunction implements Function<Integer, Text> {

	@Override
	public Text apply(Integer p_from) {
		return new Text(p_from.toString());
	}

}
