package org.seattlehadoop.ngram;

import static org.seattlehadoop.ngram.Constants.TAB;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class TokenAndCount {

	private final int m_count;
	private final String m_token;

	public TokenAndCount(String token, int count) {
		m_token = token;
		m_count = count;
	}

	public String getToken() {
		return m_token;
	}

	public int getCount() {
		return m_count;
	}

	public static Text toText(TokenAndCount tc) {
		return new Text(tc.getToken() + TAB + tc.getCount());
	}

	public static TokenAndCount fromTextSingle(Text input) {
		String[] parts = input.toString().split(String.valueOf(TAB));
		return new TokenAndCount(parts[0], Integer.parseInt(parts[1]));
	}

	public static List<TokenAndCount> parseTokenAndCountTextValues(Iterable<Text> values) {
		List<TokenAndCount> ret = new ArrayList<TokenAndCount>();
		for (Text t : values) {
			String[] secondWordAndCount = t.toString().split(String.valueOf(TAB));
			int i = 0;
			while (i < secondWordAndCount.length) {
				ret.add(new TokenAndCount(secondWordAndCount[i++], Integer.parseInt(secondWordAndCount[i++])));
			}
		}
		return ret;
	}
}
