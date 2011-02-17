package org.seattlehadoop.ngram;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TrailingWords {

	private final String m_firstWord;
	private final Map<String, Map<Integer, Integer>> m_words2countAndYears = new TreeMap<String, Map<Integer, Integer>>();

	public TrailingWords(String p_firstWord) {
		m_firstWord = p_firstWord;
	}

	@Override
	public String toString() {
		return String.format("Word: %s, Pairings: %s", m_firstWord, m_words2countAndYears);
	}

	public String getFirstWord() {
		return m_firstWord;
	}

	public Set<String> getPairedWords() {
		return m_words2countAndYears.keySet();
	}

	public Set<Integer> getYearsForPairedWord(String secondWord) {
		return m_words2countAndYears.get(secondWord).keySet();
	}

	public Integer getCountForWordInYear(String secondWord, int year) {
		return m_words2countAndYears.get(secondWord).get(year);
	}

	public void add(String secondWord, int volumeCount, int year) {
		if (!m_words2countAndYears.containsKey(secondWord)) {
			m_words2countAndYears.put(secondWord, new TreeMap<Integer, Integer>());
		}
		m_words2countAndYears.get(secondWord).put(year, volumeCount);
	}

}
