package org.seattlehadoop.ngram;

import static org.seattlehadoop.ngram.Constants.TABLE_NAME;
import static org.seattlehadoop.ngram.Constants.setHostName;
import static org.seattlehadoop.ngram.Constants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class ClientImpl implements Client {

	private HTableInterface m_hTable;

	public ClientImpl(String host) throws IOException {
		Configuration conf = setHostName(new Configuration(), host);
		setQuorum(conf, host);
		setDefaultClientPort(conf);
		m_hTable = new HTable(conf, TABLE_NAME);
	}

	@Override
	public TrailingWords getTrailingWords(String p_firstWord) throws IOException {
		TrailingWords ret = new TrailingWords(p_firstWord);
		for (KeyValue kv : m_hTable.get(new Get(Bytes.toBytes(p_firstWord)).setMaxVersions()).raw()) {
			ret.add(Bytes.toString(kv.getQualifier()), Bytes.toInt(kv.getValue()), (int) kv.getTimestamp());
		}
		return ret;
	}

	@Override
	public void close() throws IOException {
		m_hTable.close();
	}

	@Override
	public List<PairedWordScores> parseDocment(List<String> p_lines) throws IOException {
		List<PairedWordScores> ret = new ArrayList<PairedWordScores>();
		String prevLineWord = null;
		for (String line : p_lines) {
			String[] words = line.split("\\s+");
			if (words.length == 0) {
				prevLineWord = null;
				continue;
			}
			if (prevLineWord != null) {
				ret.add(new PairedWordScores(prevLineWord, words[0], getCountsByYear(prevLineWord, words[0])));
			}
			for (int i = 0; i < words.length - 1; i++) {
				ret.add(new PairedWordScores(words[i], words[i + 1], getCountsByYear(words[i], words[i + 1])));
			}
			prevLineWord = words[words.length - 1];
		}
		return ret;
	}

	@Override
	public Map<Integer, Integer> getCountsByYear(String p_firstWord, String p_secondWord) throws IOException {
		Map<Integer, Integer> ret = new HashMap<Integer, Integer>();
		for (KeyValue kv : m_hTable.get(
				new Get(Bytes.toBytes(p_firstWord)).setMaxVersions().addColumn(Constants.COLUMN_FAMILY, Bytes.toBytes(p_secondWord))).raw()) {
			// the timestamp is the year
			ret.put((int) kv.getTimestamp(), Bytes.toInt(kv.getValue()));
		}
		return ret;
	}

}
