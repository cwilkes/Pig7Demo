package org.seattlehadoop.mahout;

import static org.junit.Assert.*;

import org.junit.Test;

public class CoordAndBrightTest {

	@Test
	public void testToString() {
		Coord c = new Coord(10, -1);
		double bright = 9.8712;
		CoordAndBright cab = CoordAndBright.valueOf(new CoordAndBright(c, bright).toString());
		assertEquals(c.x, cab.getCoord().x);
		assertEquals(c.y, cab.getCoord().y);
		assertEquals(bright, cab.getBrightness(), 0.0);
	}
}
