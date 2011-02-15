package org.seattlehadoop.mahout.hadoop;

import static org.seattlehadoop.mahout.Utils.*;

public class BoxedCluster {

	private final String m_clusterName;
	private final double[] m_centers;
	private final double[] m_radiuses;

	public BoxedCluster(String p_clusterName, double[] p_centers, double[] p_radiuses) {
		m_clusterName = p_clusterName;
		m_centers = p_centers;
		m_radiuses = p_radiuses;
	}

	public String getClusterName() {
		return m_clusterName;
	}

	public double[] getCenters() {
		return m_centers;
	}

	public double[] getRadiuses() {
		return m_radiuses;
	}

	@Override
	public String toString() {
		StringBuffer ret = new StringBuffer();
		ret.append(m_clusterName);
		ret.append(TAB);
		// x
		ret.append(m_centers[0]);
		ret.append(TAB);
		// y
		ret.append(m_centers[1]);
		ret.append(TAB);
		// rectangle for coord
		ret.append(m_radiuses[0]);
		ret.append(TAB);
		ret.append(m_radiuses[1]);
		ret.append(TAB);
		// brightness
		ret.append(m_centers[2]);
		ret.append(TAB);
		ret.append(m_radiuses[2]);
		return ret.toString();
	}
}
