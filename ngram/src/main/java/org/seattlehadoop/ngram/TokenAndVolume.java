package org.seattlehadoop.ngram;

public class TokenAndVolume {

	private final int m_volumeCount;
	private final String m_token;

	public TokenAndVolume(String token, int volumeCount) {
		m_token = token;
		m_volumeCount = volumeCount;
	}

	public String getToken() {
		return m_token;
	}

	public int getVolumeCount() {
		return m_volumeCount;
	}
}
