package org.seattlehaoop.demo.cascading.distance;

import cascading.operation.AggregatorCall;
import cascading.operation.aggregator.Sum;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class SumDistance extends Sum {
	public SumDistance(String fieldName) {
		super(new Fields(fieldName));
	}

	@Override
	protected Tuple getResult(AggregatorCall<Double[]> p_aggregatorCall) {
		Tuple normal = super.getResult(p_aggregatorCall);
		return new Tuple(1 / (1 + normal.getDouble(0)));
	}
}
