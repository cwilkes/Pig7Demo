package org.seattlehadoop.ngram.input;

import org.apache.avro.util.Utf8;
import org.seattlehadoop.ngram.avro.TokenCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class TokenCountFromLineFunction implements Function<String, TokenCount> {
	private final Logger m_logger = LoggerFactory.getLogger(getClass());

	@Override
	public TokenCount apply(String p_input) {
		TokenCount m_onDeck = new TokenCount();
		try {
			int pos = 0;
			String[] parts = p_input.split("\t");
			m_onDeck.token = new Utf8(parts[pos++]);
			m_onDeck.year = Integer.parseInt(parts[pos++]);
			m_onDeck.matchCount = Integer.parseInt(parts[pos++]);
			m_onDeck.pageCount = Integer.parseInt(parts[pos++]);
			m_onDeck.volumeCount = Integer.parseInt(parts[pos++]);
			return m_onDeck;
		} catch (RuntimeException ex) {
			m_logger.warn("Error parsing '" + p_input + "' : " + ex.getMessage());
			return null;
		}
	}

}
