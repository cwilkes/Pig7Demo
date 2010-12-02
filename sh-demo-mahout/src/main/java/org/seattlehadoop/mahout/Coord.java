package org.seattlehadoop.mahout;

import static org.seattlehadoop.mahout.Utils.TAB;

public class Coord {

	public final int x, y;

	public Coord(int x, int y) {
		this.x = x;
		this.y = y;
	}

	@Override
	public String toString() {
		return x + TAB + y;
	}

	public static Coord valueOf(String line) {
		String[] parts = line.split(TAB);
		return new Coord(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
	}
}
