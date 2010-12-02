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

}
