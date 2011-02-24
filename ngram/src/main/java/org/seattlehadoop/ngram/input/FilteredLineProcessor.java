package org.seattlehadoop.ngram.input;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

/**
 * Takes an iterator of inputs and converts them into the desired output. TODO:
 * have this only take a single input and let the caller iterator through the
 * inputs
 * 
 * @author cwilkes
 * 
 * @param <IN>
 * @param <OUT>
 */
public class FilteredLineProcessor<OUT> implements Iterator<OUT>, Closeable {
	private final Function<String, OUT> m_lineToObjectFunction;

	private final BufferedReader m_br;
	private final Predicate<OUT> m_filterOutput;
	private OUT m_onDeck;
	private boolean m_done = false;

	public FilteredLineProcessor(Reader reader, Function<String, OUT> lineToObjectFunction) throws IOException {
		this(reader, lineToObjectFunction, new NotNullPredicate<OUT>());
	}

	public FilteredLineProcessor(Reader reader, Function<String, OUT> lineToObjectFunction, Predicate<OUT> filterOutput) throws IOException {
		m_br = new BufferedReader(reader);
		m_lineToObjectFunction = lineToObjectFunction;
		m_filterOutput = filterOutput;
		advance();
	}

	private void advance() throws IOException {
		String s = m_br.readLine();
		if (s == null) {
			m_done = true;
			m_br.close();
			return;
		}
		m_onDeck = m_lineToObjectFunction.apply(s);
		if (!m_filterOutput.apply(m_onDeck)) {
			advance();
		}
	}

	@Override
	public boolean hasNext() {
		return !m_done;
	}

	@Override
	public OUT next() {
		OUT ret = m_onDeck;
		m_onDeck = null;
		try {
			advance();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		if (m_br != null) {
			m_br.close();
		}
	}
}
