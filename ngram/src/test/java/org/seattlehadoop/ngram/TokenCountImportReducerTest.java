package org.seattlehadoop.ngram;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.seattlehadoop.ngram.mapreduce.Constants;
import org.seattlehadoop.ngram.mapreduce.TokenCountImportReducer;

public class TokenCountImportReducerTest {

	private TokenCountImportReducer m_reducer;

	@Before
	public void setup() {
		m_reducer = new TokenCountImportReducer();
	}

	@Test
	public void testCreateInitialPut() {
		Put put = m_reducer.createPutRequest("key", 1945);
		assertEquals("key", Bytes.toString(put.getRow()));
		assertEquals(1945, (int) put.getTimeStamp());
	}

	@Test
	public void testCreatePutColumns() {
		Put put = m_reducer.createPutRequest("key", 1945);
		assertEquals(0, put.getFamilyMap().size());
		m_reducer.addSecondWordToPutRequest(put, "cc", 11);
		m_reducer.addSecondWordToPutRequest(put, "bb", 12);
		Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
		assertEquals(1, familyMap.size());
		assertEquals(Bytes.toString(Constants.COLUMN_FAMILY), Bytes.toString(familyMap.keySet().iterator().next()));
		List<KeyValue> vals = familyMap.values().iterator().next();
		assertEquals(2, vals.size());
		// in order of inserts
		assertEquals("cc", Bytes.toString(vals.get(0).getQualifier()));
		assertEquals(11, Bytes.toInt(vals.get(0).getValue()));
		assertEquals("bb", Bytes.toString(vals.get(1).getQualifier()));
		assertEquals(12, Bytes.toInt(vals.get(1).getValue()));
	}

	@Test
	public void testHitLiveServer() throws IOException {
		ReduceDriver<Text, Text, Text, Writable> driver = new ReduceDriver<Text, Text, Text, Writable>(m_reducer);
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("bb\t1"));
		values.add(new Text("bc\t4"));
		values.add(new Text("bd\t3"));
		values.add(new Text("be\t1"));
		driver.setInput(new Text("hi\t1980"), values);
		List<Pair<Text, Writable>> output = driver.run();
		assertEquals(1, output.size());
		Put putRequest = (Put) output.get(0).getSecond();
		// row=hi, families={(family=details,
		// keyvalues=(hi/details:bb/1980/Put/vlen=4,
		// hi/details:bc/1980/Put/vlen=4, hi/details:bd/1980/Put/vlen=4,
		// hi/details:be/1980/Put/vlen=4)}
		assertEquals("hi", Bytes.toString(putRequest.getRow()));
		Map<byte[], List<KeyValue>> familyMap = putRequest.getFamilyMap();
		assertEquals(1, familyMap.size());
		assertEquals("details", Bytes.toString(familyMap.keySet().iterator().next()));
		List<KeyValue> columnValues = familyMap.values().iterator().next();
		assertEquals(4, columnValues.size());
		int i = 0;
		for (KeyValue kv : columnValues) {
			assertEquals(values.get(i).toString().split("\t")[0], Bytes.toString(kv.getQualifier()));
			assertEquals(Integer.parseInt(values.get(i).toString().split("\t")[1]), Bytes.toInt(kv.getValue()));
			i++;
		}
	}
}
