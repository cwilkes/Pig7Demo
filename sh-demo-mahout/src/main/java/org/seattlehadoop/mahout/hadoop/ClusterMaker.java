package org.seattlehadoop.mahout.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.seattlehadoop.mahout.Utils;

import com.google.common.base.Function;

public class ClusterMaker implements Function<String, Pair<Text, Cluster>> {

	private int m_lineCounter = 0;
	private final DistanceMeasure m_distanceMeasure = new SquaredEuclideanDistanceMeasure();

	public void reset() {
		m_lineCounter = 0;
	}

	@Override
	public Pair<Text, Cluster> apply(String line) {
		int keyValueSplit = line.indexOf(Utils.TAB);
		String[] parts = line.substring(keyValueSplit + 1).split("\\s+");
		int pos = 0;
		double[] value = new double[parts.length];
		while (pos < value.length) {
			value[pos] = Double.valueOf(parts[pos]);
			pos++;
		}
		String key = line.substring(0, keyValueSplit);
		return new Pair<Text, Cluster>(new Text(key), new Cluster(new DenseVector(value), m_lineCounter++, m_distanceMeasure));
	}
}
