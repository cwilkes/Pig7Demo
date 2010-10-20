package org.seattlehaoop.demo.cascading.wordcount;

import java.util.Iterator;

/**
 * Takes input like "A Bb C" and returns ("A") ("A Bb") ("Bb") ("Bb C") ("C")
 * for a 2 shingle
 * 
 * @author cwilkes
 * 
 */
public class ShingleMaker {

	private final int m_numberWords;
	private final boolean m_onlyNumberWordOutputs;

	public ShingleMaker(int p_numberOfWords, boolean p_onlyNumberWordOutputs) {
		m_numberWords = p_numberOfWords;
		m_onlyNumberWordOutputs = p_onlyNumberWordOutputs;
	}

	public Iterator<String> makeShingles(String line) {
		return new ShingleIterator(line);
	}

	private class ShingleIterator implements Iterator<String> {
		private final String[] m_parts;
		private int m_index = 0;
		private int m_currentWordCount;
		private String m_onDeck;

		public ShingleIterator(String line) {
			m_parts = line.split("\\s+");
			m_currentWordCount = 0;
			m_onDeck = "";
			advance();
		}

		@Override
		public boolean hasNext() {
			return m_onDeck != null;
		}

		@Override
		public String next() {
			String ret = m_onDeck;
			advance();
			return ret;
		}

		private int advanceExactWordCount() {
			if (m_currentWordCount == 0) {
				// just to signal the first time through
				m_currentWordCount = 1;
			} else {
				m_index++;
			}
			if (m_index + m_numberWords > m_parts.length) {
				return -1;
			}
			return m_numberWords;
		}

		private String makeToken(int wordCount) {
			if (wordCount == -1) {
				return null;
			}
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < wordCount; i++) {
				if (sb.length() > 0) {
					sb.append(" ");
				}
				sb.append(m_parts[m_index + i]);
			}
			return sb.toString();
		}

		private int advanceMultiWords() {
			if (m_currentWordCount >= m_numberWords || m_index + m_currentWordCount >= m_parts.length) {
				m_currentWordCount = 1;
				m_index++;
			} else {
				m_currentWordCount++;
			}
			if (m_index >= m_parts.length) {
				return -1;
			}
			return m_currentWordCount;
		}

		private void advance() {
			int tokenSize = m_onlyNumberWordOutputs ? advanceExactWordCount() : advanceMultiWords();
			m_onDeck = makeToken(tokenSize);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
