package org.seattlehadoop.ngram;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TokenCountImportReducerTest {

	private final HTableInterface m_htable = createMock(HTableInterface.class);
	private TokenCountImportReducer m_reducer;

	@Before
	public void setup() {
		reset(m_htable);
		m_reducer = new TokenCountImportReducer();
		m_reducer.setHtable(m_htable);
	}

	@Test
	public void testSingleRow() throws IOException, InterruptedException {
		// should really test what is added
		m_htable.put(isA(Put.class));
		expectLastCall();
		m_htable.close();
		expectLastCall();
		replay(m_htable);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("bb\t1"));
		values.add(new Text("bc\t4"));
		values.add(new Text("bd\t3"));
		values.add(new Text("be\t1"));
		m_reducer.reduce(new Text("hi\t1972"), values, null);
		m_reducer.cleanup(null);
	}

	@Test
	public void testHitLiveServer() throws IOException {
		ReduceDriver<Text, Text, Text, IntWritable> driver = new ReduceDriver<Text, Text, Text, IntWritable>(m_reducer);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("bb\t1"));
		values.add(new Text("bc\t4"));
		values.add(new Text("bd\t3"));
		values.add(new Text("be\t1"));
		driver.setInput(new Text("hi\t1980"), values);
		driver.run();
	}
}
