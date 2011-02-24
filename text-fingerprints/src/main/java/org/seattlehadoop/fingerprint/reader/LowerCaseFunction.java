package org.seattlehadoop.fingerprint.reader;

import com.google.common.base.Function;

public class LowerCaseFunction implements Function<String, String> {

	@Override
	public String apply(String p_input) {
		return p_input.toLowerCase();
	}

}
