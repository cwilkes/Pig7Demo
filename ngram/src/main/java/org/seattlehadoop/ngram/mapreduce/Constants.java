package org.seattlehadoop.ngram.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class Constants {

	public static char TAB = '\t';

	public static String TABLE_NAME = "tokens";
	public static byte[] COLUMN_FAMILY = Bytes.toBytes("details");

	private static final String ZK_PROPERTY_PREFIX = "hbase.zookeeper.property.";

	public static Configuration setDefaultClientPort(Configuration conf) {
		return setClientPort(conf, 2181);
	}

	public static Configuration setClientPort(Configuration conf, int port) {
		conf.setInt(ZK_PROPERTY_PREFIX + "clientPort", port);
		return conf;
	}

	public static Configuration setHostName(Configuration conf, String hostName) {
		conf.set(ZK_PROPERTY_PREFIX + "server.foo", hostName + ":bar");
		return conf;
	}

	public static Configuration setQuorum(Configuration conf, String hostName) {
		conf.set(HConstants.ZOOKEEPER_QUORUM, hostName);
		return conf;
	}
}
