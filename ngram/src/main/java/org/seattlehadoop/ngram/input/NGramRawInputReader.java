package org.seattlehadoop.ngram.input;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.zip.ZipInputStream;


public class NGramRawInputReader implements Iterator<TokenAndCounts>, Closeable {

	private final BufferedReader m_br;
	private TokenAndCounts m_onDeck;
	private String[] m_prevLine;
	private final int m_minYear;
	private final int m_maxYear;

	public NGramRawInputReader(Reader reader) throws IOException {
		m_br = new BufferedReader(reader);
		m_minYear = Integer.MIN_VALUE;
		m_maxYear = Integer.MAX_VALUE;
		advance();
	}

	public NGramRawInputReader(File in, int minYear, int maxYear) throws IOException {
		m_minYear = minYear;
		m_maxYear = maxYear;
		if (in.getName().endsWith(".zip")) {
			ZipInputStream zis = new ZipInputStream(new FileInputStream(in));
			// just do one entry for now
			zis.getNextEntry();
			m_br = new BufferedReader(new InputStreamReader(zis));
		} else {
			m_br = new BufferedReader(new FileReader(in));
		}
		advance();
	}

	private boolean isYearOkay(int year) {
		return year >= m_minYear && year <= m_maxYear;
	}

	private void advance() throws IOException {
		String s = null;
		if (m_prevLine == null) {
			m_onDeck = null;
		} else {
			m_onDeck = new TokenAndCounts(m_prevLine);
			m_prevLine = null;
		}
		while ((s = m_br.readLine()) != null) {
			m_prevLine = s.split("\\t");
			int year = TokenAndCounts.parseNumbers(m_prevLine)[0];
			if (!isYearOkay(year)) {
				m_prevLine = null;
				continue;
			}
			if (m_onDeck != null && !m_prevLine[0].equals(m_onDeck.getToken())) {
				break;
			}
			if (m_onDeck == null) {
				if (!isTokenOkay(m_prevLine[0])) {
					continue;
				}
				m_onDeck = new TokenAndCounts(m_prevLine);
			} else {
				m_onDeck.add(m_prevLine);
			}
			m_prevLine = null;
		}
		if (m_onDeck == null) {
			System.err.println("Closing input");
			close();
		}
		// System.err.println("Read in " + m_onDeck);
	}

	private boolean isTokenOkay(String token) {
		if (!Character.isLetterOrDigit(token.charAt(0))) {
			return false;
		}
		return Character.isLetterOrDigit(token.charAt(token.length() - 1));
	}

	@Override
	public boolean hasNext() {
		return m_onDeck != null;
	}

	@Override
	public TokenAndCounts next() {
		TokenAndCounts ret = m_onDeck;
		try {
			advance();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		m_br.close();
	}

}
