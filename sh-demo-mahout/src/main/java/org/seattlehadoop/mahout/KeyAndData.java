package org.seattlehadoop.mahout;

public class KeyAndData<T> {

	private final ClusterKey m_key;
	private final T m_data;

	public KeyAndData(ClusterKey key, T data) {
		m_key = key;
		m_data = data;
	}

	public ClusterKey getKey() {
		return m_key;
	}

	public T getData() {
		return m_data;
	}

}
