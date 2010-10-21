package org.seattlehaoop.demo.cascading.distance;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * takes a {@link Tuple} that has a double in the first and second columns and
 * emits the square of the difference between the two
 * 
 * @author cwilkes
 * 
 */
@SuppressWarnings("serial")
public class SquareDiff extends Identity {
	public SquareDiff(String fieldName) {
		super(new Fields(fieldName));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry input = functionCall.getArguments();
		functionCall.getOutputCollector().add(new Tuple(Math.pow(input.getTuple().getDouble(0) - input.getTuple().getDouble(1), 2)));
	}
}