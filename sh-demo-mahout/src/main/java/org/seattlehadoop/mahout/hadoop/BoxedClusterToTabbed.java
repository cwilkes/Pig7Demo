package org.seattlehadoop.mahout.hadoop;

import static org.seattlehadoop.mahout.Utils.TAB;

import com.google.common.base.Function;

public class BoxedClusterToTabbed implements Function<BoxedCluster, String> {

	@Override
	public String apply(BoxedCluster bc) {
		return bc.getClusterName() + TAB + bc.getCenters()[0] + TAB + bc.getCenters()[1] + TAB + bc.getRadiuses()[0] + TAB + bc.getRadiuses()[1] + TAB
				+ bc.getCenters()[2];
	}
}
