package org.seattlehaoop.demo.cascading.wordcount;

import java.beans.ConstructorProperties;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ShingleFunction extends BaseOperation<ShingleMaker> implements Function<ShingleMaker> {

	private static final long serialVersionUID = 663772921616891638L;
	private final int m_numberOfWords;
	private final boolean m_onlyNumberWordOutputs;

	@ConstructorProperties({ "fieldDeclaration", "numberOfWords" })
	public ShingleFunction(Fields fieldDeclaration, int numberOfWords, boolean onlyNumberWordOutputs) {
		super(1, fieldDeclaration);
		m_numberOfWords = numberOfWords;
		m_onlyNumberWordOutputs = onlyNumberWordOutputs;
	}

	public int getNumberOfWords() {
		return m_numberOfWords;
	}

	@Override
	public void prepare(FlowProcess p_flowProcess, OperationCall<ShingleMaker> p_operationCall) {
		p_operationCall.setContext(new ShingleMaker(getNumberOfWords(), m_onlyNumberWordOutputs));
	}

	@Override
	public void operate(FlowProcess p_flowProcess, FunctionCall<ShingleMaker> functionCall) {
		String line = functionCall.getArguments().getString(0);
		Iterator<String> shingles = functionCall.getContext().makeShingles(line);
		while (shingles.hasNext()) {
			Tuple tuple = new Tuple(shingles.next());			
			System.out.println(tuple);
			functionCall.getOutputCollector().add(tuple);
		}

	}
}
