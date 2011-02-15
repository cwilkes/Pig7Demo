package org.seattlehadoop.mahout.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;

import com.google.common.base.Function;

public class ClusterTransformer implements Function<Pair<Text, Cluster>, BoxedCluster> {

	private final Function<Vector, double[]> m_vector2double = new VectorToDoubleTransformer();

	@Override
	public BoxedCluster apply(Pair<Text, Cluster> p_from) {
		String clusterName = p_from.getFirst().toString();
		double[] centers = m_vector2double.apply(p_from.getSecond().getCenter());
		double[] radiuses = m_vector2double.apply(p_from.getSecond().getRadius());
		return new BoxedCluster(clusterName, centers, radiuses);
	}
}
