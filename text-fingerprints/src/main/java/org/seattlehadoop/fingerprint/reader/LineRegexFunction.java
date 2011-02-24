package org.seattlehadoop.fingerprint.reader;

import com.google.common.base.Function;

public class LineRegexFunction implements Function<String, String> {

	private final String m_pattern;
	private final String m_replacement;

	public LineRegexFunction(String pattern, String replacement) {
		m_pattern = pattern;
		m_replacement = replacement;
	}

	@Override
	public String apply(String p_input) {
		return p_input.replaceAll(m_pattern, m_replacement);
	}
}
