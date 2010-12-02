package org.seattlehadoop.mahout;

import static org.seattlehadoop.mahout.Utils.TAB;

import java.util.Iterator;
import java.util.Random;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class PlanetCluster2 implements ClusterService<CoordAndBright> {

	@Override
	public Iterator<CoordAndBright> createPlanetInCluster(PlanetCreationOptions options, final Random r) {
		final double baseBright = 25 + 40 * (r.nextDouble() - 0.5);
		return Iterators.transform(new PlanetCluster1().createPlanetInCluster(options, r), new Function<Coord, CoordAndBright>() {

			@Override
			public CoordAndBright apply(Coord p_from) {
				return new CoordAndBright(p_from, baseBright + 2 * (r.nextDouble() - 1));
			}
		});
	}

	@Override
	public String getHeader() {
		return "X" + TAB + "Y" + TAB + "BRIGHT";
	}

}
