package org.seattlehadoop.ngram;

import static org.seattlehadoop.ngram.Constants.TABLE_NAME;
import static org.seattlehadoop.ngram.Constants.setHostName;
import static org.seattlehadoop.ngram.Constants.*;

import java.io.IOException;

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

}
