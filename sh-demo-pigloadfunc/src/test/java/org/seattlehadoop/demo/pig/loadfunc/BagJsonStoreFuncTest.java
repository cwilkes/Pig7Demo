package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class BagJsonStoreFuncTest {

	protected Tuple createSaleRecord(long userId, long itemId, float price) throws ExecException {
		Tuple t2 = TupleFactory.getInstance().newTuple(3);
		t2.set(0, userId);
		t2.set(1, itemId);
		t2.set(2, price);
		return t2;
	}

	@Test
	public void testSerialize() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(2);
		DefaultDataBag bag = new DefaultDataBag();
		bag.add(createSaleRecord(1L, 100L, 56.5F));
		bag.add(createSaleRecord(1L, 101L, 2.5F));
		t.set(0, 1L);
		t.set(1, bag);
		BagJsonStoreFunc storeFunc = new BagJsonStoreFunc("userId", "purchases", "", "itemId", "profit");
		InMemoryRecordWriter<Text, Text> writer = new InMemoryRecordWriter<Text, Text>();
		storeFunc.prepareToWrite(writer);
		storeFunc.putNext(t);
		System.out.println(writer.getNextKey());
	}
}
