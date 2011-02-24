package org.seattlehadoop.fingerprint.reader;

import com.google.common.base.Function;

public class PadStringFunction implements Function<String, String> {

	private final String m_padding;

	public PadStringFunction(int tokenSize) {
		StringBuffer sb = new StringBuffer();
		while (tokenSize-- >= 0) {
			sb.append(" ");
		}
		m_padding = sb.toString();
	}

	public PadStringFunction() {
		this(" ");
	}

	public PadStringFunction(String padding) {
		m_padding = padding;
	}

	@Override
	public String apply(String p_input) {
		return m_padding + p_input + m_padding;
	}

}