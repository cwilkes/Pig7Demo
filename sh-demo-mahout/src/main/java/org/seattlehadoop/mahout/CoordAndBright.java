package org.seattlehadoop.mahout;

import static org.seattlehadoop.mahout.Utils.TAB;

public class CoordAndBright {

	private final double m_brightness;
	private final Coord m_coord;

	public CoordAndBright(Coord center, double brightness) {
		m_coord = center;
		m_brightness = brightness;
	}

	@Override
	public String toString() {
		return m_coord + TAB + m_brightness;
	}

	public Coord getCoord() {
		return m_coord;
	}

	public double getBrightness() {
		return m_brightness;
	}
}
