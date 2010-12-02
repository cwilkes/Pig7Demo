package org.seattlehadoop.mahout;

public class ClusterKey {

	private final String m_clusterName;
	private final int m_itemNumber;

	public ClusterKey(String clusterName, int itemNumber) {
		m_clusterName = clusterName;
		m_itemNumber = itemNumber;
	}

	public String getClusterName() {
		return m_clusterName;
	}

	public int getItemNumber() {
		return m_itemNumber;
	}

	@Override
	public String toString() {
		return m_clusterName + Utils.TAB + m_clusterName + "-" + m_itemNumber;
	}

	public static ClusterKey makeFromString(String line) {
		line = line.substring(line.lastIndexOf(Utils.TAB) + 1);
		String itemNumber = line.substring(line.lastIndexOf("-"));
		return new ClusterKey(line.substring(0, line.length() - itemNumber.length()), Integer.parseInt(itemNumber.substring(1)));
	}

}
