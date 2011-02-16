package org.seattlehadoop.ngram;

import java.util.ArrayList;
import java.util.List;

public class TokenAndCounts {

	private final String m_token;
	private final List<int[]> m_counts;

	public TokenAndCounts(String token, List<int[]> counts) {
		m_token = token.toLowerCase();
		m_counts = counts;
	}

	public List<int[]> getCounts() {
		return m_counts;
	};

	public TokenAndCounts(String[] input) {
		int i = 0;
		m_token = input[i++].toLowerCase();
		m_counts = new ArrayList<int[]>();
		m_counts.add(new int[] { Integer.parseInt(input[i++]), Integer.parseInt(input[i++]), Integer.parseInt(input[i++]), Integer.parseInt(input[i++]) });
	}

	public TokenAndCounts(String token, int year, int matchCount, int pageCount, int volumeCount) {
		m_token = token.toLowerCase();
		m_counts = new ArrayList<int[]>();
		m_counts.add(new int[] { year, matchCount, pageCount, volumeCount });
	}

	@Override
	public String toString() {
		return String.format("Token: %s, #Entries: %d", m_token, m_counts.size());
	}

	public String getToken() {
		return m_token;
	}

	public static int[] parseNumbers(String[] input) {
		int i = 1;
		return new int[] { Integer.parseInt(input[i++]), Integer.parseInt(input[i++]), Integer.parseInt(input[i++]), Integer.parseInt(input[i++]) };
	}

	public void add(String[] input) {
		add(parseNumbers(input));
	}

	public void add(int[] p_parseNumbers) {
		m_counts.add(p_parseNumbers);
	}
}
