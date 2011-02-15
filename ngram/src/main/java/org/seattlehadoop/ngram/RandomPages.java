package org.seattlehadoop.ngram;

import java.util.Iterator;
import java.util.Random;

public class RandomPages implements Iterator<WebPage> {

	private final int m_totalRecords;
	private final int m_maxID;
	private final String[] m_pages;
	private int m_currentRecordNumber = 0;
	private final Random m_rand;

	public RandomPages(int p_totalRecords, int p_maxID, String... p_pages) {
		m_totalRecords = p_totalRecords;
		m_maxID = p_maxID;
		m_pages = p_pages;
		m_rand = new Random();
	}

	@Override
	public boolean hasNext() {
		return m_currentRecordNumber < m_totalRecords;
	}

	@Override
	public WebPage next() {
		return new WebPage(m_rand.nextInt(m_maxID) + 1, m_pages[m_rand.nextInt(m_pages.length)]);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
