package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InMemoryRecordWriter<K, V> extends RecordWriter<K, V> {

	private final List<Object[]> m_values = new ArrayList<Object[]>();

	@Override
	public void write(K p_key, V p_value) throws IOException, InterruptedException {
		m_values.add(new Object[] { p_key, p_value });
	}

	public boolean hasNext() {
		return m_values.size() > 0;
	}

	public void advance() {
		m_values.remove(0);
	}

	@SuppressWarnings("unchecked")
	public K getNextKey() {
		return (K) m_values.get(0)[0];
	}

	@SuppressWarnings("unchecked")
	public V getNextValue() {
		return (V) m_values.get(0)[1];
	}

	@Override
	public void close(TaskAttemptContext p_context) throws IOException, InterruptedException {
		// ignore
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Object[] o : m_values) {
			sb.append(o[0] + ":" + o[1] + ", ");
		}
		return "#" + m_values.size() + " : " + sb.toString();
	}
}
