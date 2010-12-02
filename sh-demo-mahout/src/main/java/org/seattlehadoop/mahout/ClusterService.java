package org.seattlehadoop.mahout;

import java.util.Iterator;
import java.util.Random;

public interface ClusterService<T> {

	Iterator<T> createPlanetInCluster(PlanetCreationOptions options, Random r);

	String getHeader();
}
