package org.seattlehadoop.mahout.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.seattlehadoop.mahout.Utils;

import com.google.common.base.Function;

public class CoordAndBrightnessConverter implements Function<String, Pair<Text, VectorWritable>> {

	@Override
	public Pair<Text, VectorWritable> apply(String line) {
		String[] parts = line.split(Utils.TAB);
		int pos = 0;
		String clusterName = parts[pos++];
		pos++;
		int x = Integer.parseInt(parts[pos++]);
		int y = Integer.parseInt(parts[pos++]);
		double brightNess = Double.parseDouble(parts[pos++]);
		return new Pair<Text, VectorWritable>(new Text(clusterName), new VectorWritable(new DenseVector(new double[] { x, y, brightNess })));
	}
}
