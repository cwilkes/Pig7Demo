package org.seattlehadoop.mahout.serialization;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.seattlehadoop.mahout.CoordAndBright;

import com.google.common.base.Function;

public class CoordAndBrightoVectorFunction implements Function<CoordAndBright, VectorWritable> {

	@Override
	public VectorWritable apply(CoordAndBright cab) {
		return new VectorWritable(new DenseVector(new double[] { cab.getCoord().x, cab.getCoord().y, cab.getBrightness() }));
	}

}
