package org.seattlehaoop.demo.cascading.distance;

import static org.seattlehaoop.demo.cascading.distance.FlowUtils.makeLocalFlow;
import cascading.flow.Flow;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MovieDistances {

	private static final String MOVIE = "movie";
	
	public static Pipe makePipe() {
		Pipe pipe = new Pipe("movieReviews");

		// line is tab delimited
		pipe = new Each(pipe, new Fields("line"), new RegexSplitter("\t"));

		// takes "JimBob Mo1 1.2 Mo2 2.3 Mo3 9.9" and splits into 3 different
		// rows, one for each (movie,rate) pair
		// also puts field names on the resulting fields
		pipe = new Each(pipe, new UnGroup(new Fields("name", MOVIE, "rate"), new Fields(0), 2));

		return pipe;
	}

	public static void main(String[] args) throws Exception {
		Flow flow = makeLocalFlow(args[0], args[1] + "/long", makePipe());

		flow.complete();
	}

}
