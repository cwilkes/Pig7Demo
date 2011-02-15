package org.seattlehadoop.mahout.hadoop;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import com.google.common.base.Function;

public class VectorToDoubleTransformer implements Function<Vector, double[]> {

	@Override
	public double[] apply(Vector from) {
		DenseVector vector = (DenseVector) from;
		double[] value = new double[vector.getNumNondefaultElements()];
		for (int i = 0; i < value.length; i++) {
			value[i] = vector.get(i);
		}
		return value;
	}
}
