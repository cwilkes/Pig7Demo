package org.seattlehadoop.ngram;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Importer1 {

	private final HTable m_htable;

	public Importer1(Configuration conf, String tableName) throws IOException {
		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		m_htable = new HTable(hbaseConfig, tableName);
		m_htable.setAutoFlush(false);
		m_htable.setWriteBufferSize(1024 * 1024 * 12);
	}

	public Importer1() throws IOException {
		this(new Configuration(), "access_logs");
	}

	public static void main(String[] args) throws Exception {
		int totalRecords = 100000;
		int maxID = totalRecords / 1000;
		String[] pageNames = { "/", "/a.html", "/b.html", "/c.html" };

		Iterator<WebPage> pages = new RandomPages(totalRecords, maxID, pageNames);

		System.out.println("importing " + totalRecords + " records ....");

		Importer1 importer = new Importer1();
		int i = 0;
		while (pages.hasNext()) {
			WebPage webPage = pages.next();
			importer.add(webPage.getUserID(), i++, webPage.getPageName());
		}
		importer.close();
		System.out.println("done");
	}

	public void close() throws IOException {
		m_htable.flushCommits();
		m_htable.close();
	}

	public void add(int userID, int increasing, String randomPage) throws IOException {
		Put put = new Put(Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(increasing)));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("page"), Bytes.toBytes(randomPage));
		m_htable.put(put);
	}
}
