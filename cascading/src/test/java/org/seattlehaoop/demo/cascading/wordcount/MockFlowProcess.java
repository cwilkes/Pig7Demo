package org.seattlehaoop.demo.cascading.wordcount;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class MockFlowProcess extends FlowProcess {

	@Override
	public Object getProperty(String p_key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void keepAlive() {
		// TODO Auto-generated method stub

	}

	@Override
	public void increment(Enum p_counter, int p_amount) {
		// TODO Auto-generated method stub

	}

	@Override
	public void increment(String p_group, String p_counter, int p_amount) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStatus(String p_status) {
		// TODO Auto-generated method stub

	}

	@Override
	public TupleEntryIterator openTapForRead(Tap p_tap) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleEntryCollector openTapForWrite(Tap p_tap) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}