package org.seattlehadoop.fingerprint.reader;

import java.util.Iterator;

public class ParagraphMaker implements Iterator<String> {

	private String m_ondeck, m_prev;
	private final Iterator<String> m_strings;

	public ParagraphMaker(Iterator<String> strings) {
		m_strings = strings;
		m_prev = "";
		advance();
	}

	private boolean isParagraphBreak(String line) {
		if (line.trim().isEmpty()) {
			return true;
		}
		char c = line.charAt(0);
		return c == ' ' || c == '\t';
	}

	private void advance() {
		StringBuffer sb = new StringBuffer(m_prev);
		m_prev = "";
		while (m_strings.hasNext()) {
			String s = m_strings.next();
			if (isParagraphBreak(s)) {
				m_ondeck = sb.toString().trim();
				if (!m_ondeck.isEmpty()) {
					m_prev = s.trim();
					break;
				}
				sb.append(" " + s);
			} else {
				sb.append(" " + s);
			}
		}
		m_ondeck = sb.toString().trim();
		if (m_ondeck.isEmpty()) {
			m_ondeck = null;
		}
	}

	@Override
	public boolean hasNext() {
		return m_ondeck != null;
	}

	@Override
	public String next() {
		String ret = m_ondeck;
		m_ondeck = null;
		advance();
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
