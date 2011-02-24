package org.seattlehadoop.fingerprint.reader;

import com.google.common.base.Predicate;

public class AcceptableLinePredicate implements Predicate<String> {

	private final int m_minLength;

	public AcceptableLinePredicate(int minLength) {
		m_minLength = minLength;
	}

	@Override
	public boolean apply(String p_input) {
		return p_input.length() >= m_minLength;
	}

}
