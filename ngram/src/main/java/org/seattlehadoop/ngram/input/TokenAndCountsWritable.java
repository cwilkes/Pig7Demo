package org.seattlehadoop.ngram.input;

import static org.apache.hadoop.io.WritableUtils.readString;
import static org.apache.hadoop.io.WritableUtils.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class TokenAndCountsWritable implements Writable {

	private final CountsArray m_ca;
	private List<int[]> m_counts;
	private String m_token;

	public TokenAndCountsWritable(TokenAndCounts p_tokenAndCounts) {
		this();
		m_token = p_tokenAndCounts.getToken();
		m_counts = p_tokenAndCounts.getCounts();
	}

	public TokenAndCountsWritable() {
		m_ca = new CountsArray();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writeString(out, m_token);
		new CountsArray(m_counts).write(out);
	}

	public TokenAndCounts getTokenAndCounts() {
		return new TokenAndCounts(m_token, m_counts);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_token = readString(in);
		m_ca.readFields(in);
		m_counts = m_ca.getCounts();
	}

}
