package org.seattlehadoop.fingerprint.reader;

import java.util.Iterator;

public class ShingleMaker implements Iterator<String> {

	private final Iterator<String> m_lines;
	private final int m_numberLettersInShingle;
	private int m_posInCurrentLine = 0;
	private String m_currentLine;

	public ShingleMaker(Iterator<String> lines, int numberLettersInShingle) {
		m_lines = lines;
		m_numberLettersInShingle = numberLettersInShingle;
		m_currentLine = "";
		advance();
	}

	private void advance() {
		if (m_posInCurrentLine < m_currentLine.length() - m_numberLettersInShingle) {
			m_posInCurrentLine++;
			return;
		}
		while (m_lines.hasNext()) {
			m_currentLine = m_lines.next().replaceAll("\\s+", " ");
			if (m_currentLine.length() >= 3) {
				m_currentLine = " " + m_currentLine + " ";
				m_posInCurrentLine = 0;
				return;
			}
		}
		m_currentLine = null;
	}

	@Override
	public boolean hasNext() {
		return m_currentLine != null;
	}

	@Override
	public String next() {
		String ret = m_currentLine.substring(m_posInCurrentLine, m_posInCurrentLine + m_numberLettersInShingle);
		advance();
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
