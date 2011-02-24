package org.seattlehadoop.fingerprint.reader;

import java.util.Iterator;

import com.google.common.base.Function;

public class CumulativeIterator<T> implements Iterator<T> {

	private final Iterator<T> m_input;
	private final Function<T, Iterator<T>> m_multiplier;
	private Iterator<T> m_ondeck;
	private T m_toReturn;
	private boolean m_done;

	public static <T> Iterator<T> makeIterator(Iterator<T> input, Function<T, Iterable<T>> multiplier) {
		return null;
	}

	public CumulativeIterator(Iterator<T> input, Function<T, Iterator<T>> multiplier) {
		m_input = input;
		m_multiplier = multiplier;
		advance();
	}

	private void advance() {
		if (m_ondeck == null) {
			if (!m_input.hasNext()) {
				m_done = true;
				return;
			}
			m_ondeck = m_multiplier.apply(m_input.next());
		}
		if (m_ondeck.hasNext()) {
			m_toReturn = m_ondeck.next();
			return;
		}
		m_ondeck = null;
		advance();
	}

	@Override
	public boolean hasNext() {
		return !m_done;
	}

	@Override
	public T next() {
		T ret = m_toReturn;
		advance();
		return ret;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}
}
