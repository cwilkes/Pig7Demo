package org.seattlehaoop.demo.cascading.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class MockFunctionCall<C> implements FunctionCall<C> {

	private C m_context;
	private List<Tuple> m_tuples = new ArrayList<Tuple>();
	private List<String> m_argumentStrings;

	public MockFunctionCall<C> setArgumentString(String single) {
		return setArgumentStrings(Arrays.asList(single));
	}

	public MockFunctionCall<C> setArgumentStrings(List<String> p_argumentStrings) {
		m_argumentStrings = p_argumentStrings;
		return this;
	}

	@Override
	public C getContext() {
		return m_context;
	}

	@Override
	public void setContext(C context) {
		m_context = context;
	}

	@Override
	public Fields getArgumentFields() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleEntry getArguments() {
		return new TupleEntry() {
			@Override
			public String getString(Comparable p_fieldName) {
				Integer pos = (Integer) p_fieldName;
				return m_argumentStrings.get(pos);
			}
		};
	}

	public List<Tuple> getTuples() {
		return m_tuples;
	}

	@Override
	public TupleEntryCollector getOutputCollector() {
		return new TupleEntryCollector() {

			@Override
			protected void collect(Tuple p_tuple) {
				m_tuples.add(p_tuple);
			}
		};
	}

}
