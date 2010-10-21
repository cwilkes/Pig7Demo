package org.seattlehaoop.demo.cascading.distance;

import static org.seattlehaoop.demo.cascading.distance.FlowUtils.makeLocalFlow;
import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MovieDistances {

	/**
	 * First part is the movie name, second is the leftReviewer (and is the
	 * matching column) and third is the score. This is repeated for the right
	 * reviewer and score. The \\1 reference refers back to the left reviewer.
	 */
	private static final String MOVIE_RATING_REGEX = "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*";

	private static final String MOVIE = "movie";
	private final static String PERSON_LEFT = "nameLeft";
	private final static String PERSON_RIGHT = "nameRight";

	private static final String RATE_LEFT = "rateLeft";
	private static final String RATE_RIGHT = "rateRight";

	public static Pipe makePipe() {
		Pipe pipe = new Pipe("movieReviews");

		// line is tab delimited
		pipe = new Each(pipe, new Fields("line"), new RegexSplitter("\t"));

		// takes "JimBob Mo1 1.2 Mo2 2.3 Mo3 9.9" and splits into 3 different
		// rows, one for each (movie,rate) pair
		// also puts field names on the resulting fields
		pipe = new Each(pipe, new UnGroup(new Fields("name", MOVIE, "rate"), new Fields(0), 2));

		// now take the movie column and join all fields on that, such that each
		// row now has 6 columns total --
		// two pairs of (name,movie,rating)
		pipe = new CoGroup(pipe, new Fields(MOVIE), 1, new Fields(PERSON_LEFT, MOVIE, RATE_LEFT, PERSON_RIGHT, "movieRight", RATE_RIGHT));

		// the movie column is repeated in "movieRight", this also shows what
		// columns we care about
		pipe = new Each(pipe, new Fields(MOVIE, PERSON_LEFT, RATE_LEFT, PERSON_RIGHT, RATE_RIGHT), new Identity());

		// the cogroup means that a reviewer was grouped against themselves
		pipe = new Each(pipe, new RegexFilter(MOVIE_RATING_REGEX, true));

		return pipe;
	}

	public static void main(String[] args) throws Exception {
		Flow flow = makeLocalFlow(args[0], args[1] + "/long", makePipe());

		flow.complete();
	}

}
