package org.seattlehadoop.ngram.mapreduce;

import java.util.Map;

public class PairedWordScores {

	private final String m_firstWord;
	private final String m_secondWord;
	private final Map<Integer, Integer> m_countsByYear;

	public PairedWordScores(String firstWord, String secondWord, Map<Integer, Integer> p_countsByYear) {
		m_firstWord = firstWord;
		m_secondWord = secondWord;
		m_countsByYear = p_countsByYear;
	}

	public String getFirstWord() {
		return m_firstWord;
	}

	public String getSecondWord() {
		return m_secondWord;
	}

	public Map<Integer, Integer> getCountsByYear() {
		return m_countsByYear;
	}
}
