package org.seattlehadoop.mahout;

import com.google.common.base.Function;

public class Utils {

	public static final String TAB = "\t";
	public static final String NEWLINE = System.getProperty("line.separator");
	private static final String MINI_CLASS_NAME_PREFIX = "sh.";
	public static Function<String, Integer> STRING_TO_INTEGER = new StringToInteger();
	public static Function<String, Coord> STRING_TO_COORD = new StringToCoord();
	public static Function<Coord, String> COORD_TO_STRING = new CoordToString();

	@SuppressWarnings("unchecked")
	public static <T> ClusterService<T> createClusterConverter(String clusterName) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		final Class<ClusterService<T>> clazz;
		if (clusterName.startsWith(MINI_CLASS_NAME_PREFIX)) {
			clazz = (Class<ClusterService<T>>) Class.forName(PlanetCluster1.class.getPackage().getName() + "."
					+ clusterName.substring(MINI_CLASS_NAME_PREFIX.length()));
		} else {
			clazz = (Class<ClusterService<T>>) Class.forName(clusterName);
		}
		return clazz.newInstance();
	}

	private static class StringToInteger implements Function<String, Integer> {

		@Override
		public Integer apply(String p_from) {
			return Integer.parseInt(p_from);
		}

	}

	private static class CoordToString implements Function<Coord, String> {

		@Override
		public String apply(Coord coord) {
			return coord.x + TAB + coord.y;
		}
	}

	private static class StringToCoord implements Function<String, Coord> {

		@Override
		public Coord apply(String p_from) {
			String[] parts = p_from.split(TAB);
			return new Coord(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
		}
	}
}
