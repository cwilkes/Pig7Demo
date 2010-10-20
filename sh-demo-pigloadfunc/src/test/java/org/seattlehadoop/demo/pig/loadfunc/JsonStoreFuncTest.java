package org.seattlehadoop.demo.pig.loadfunc;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class JsonStoreFuncTest {

	@Test
	public void testSerialize() throws IOException {
		StoreFunc storeFunc = new JsonStoreFunc("a", "c", "b");
		InMemoryRecordWriter<Text, Text> writer = new InMemoryRecordWriter<Text, Text>();
		storeFunc.prepareToWrite(writer);
		Tuple t = TupleFactory.getInstance().newTuple(3);
		t.set(0, "aa");
		t.set(1, "true");
		t.set(2, 10);
		storeFunc.putNext(t);
		
		assertTrue(writer.hasNext());
		assertEquals("{\"a\":\"aa\",\"c\":\"true\",\"b\":\"10\"}", writer.getNextKey().toString());
		writer.advance();
		assertFalse(writer.hasNext());

	}
}
