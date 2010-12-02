package org.seattlehadoop.mahout;

import static org.seattlehadoop.mahout.Utils.TAB;

import java.util.Iterator;
import java.util.Random;

public class PlanetCluster1 implements ClusterService<Coord> {

	@Override
	public Iterator<Coord> createPlanetInCluster(PlanetCreationOptions options, Random r) {
		return new PlanetCluster1CreatorIterator(options.getBodies(), options.getRadius(), options.getGridSize(), r);
	}

	private static class PlanetCluster1CreatorIterator implements Iterator<Coord> {

		private int m_bodiesToGo;
		private final int m_radius;
		private final Random m_random;
		private int m_centerX;
		private int m_centerY;

		public PlanetCluster1CreatorIterator(int p_bodies, int p_radius, int p_gridSize, Random p_random) {
			m_bodiesToGo = p_bodies;
			m_radius = p_radius;
			m_random = p_random;
			m_centerX = m_radius + m_random.nextInt(p_gridSize - m_radius);
			m_centerY = m_radius + m_random.nextInt(p_gridSize - m_radius);
		}

		@Override
		public boolean hasNext() {
			return m_bodiesToGo > 0;
		}

		@Override
		public Coord next() {
			m_bodiesToGo--;
			int x = (int) (m_centerX + 2 * m_radius * (m_random.nextDouble() - 0.5));
			int y = (int) (m_centerY + 2 * m_radius * (m_random.nextDouble() - 0.5));
			return new Coord(x, y);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public String getHeader() {
		return "X" + TAB + "Y";
	}

}
