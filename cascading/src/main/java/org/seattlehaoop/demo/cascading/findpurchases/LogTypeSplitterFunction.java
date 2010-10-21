package org.seattlehaoop.demo.cascading.findpurchases;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;

public class LogTypeSplitterFunction extends BaseOperation<LogTypeSplitter> implements Function<LogTypeSplitter> {

	@Override
	public void prepare(FlowProcess p_flowProcess, OperationCall<LogTypeSplitter> operationCall) {
		operationCall.setContext(new LogTypeSplitter());
	}

	@Override
	public void operate(FlowProcess p_flowProcess, FunctionCall<LogTypeSplitter> functionCall) {
		String line = functionCall.getArguments().getString(0);
		LogEntry logEntry = functionCall.getContext().codeLogLine(line);
		final Tuple tuple;
		if (logEntry.isPurchaseRecord()) {
			//tuple = new Tuple(values)
		} else {
			
		}
	}

}
