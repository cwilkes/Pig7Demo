package org.seattlehaoop.demo.cascading.distance;

import static org.seattlehaoop.demo.cascading.distance.FlowUtils.makeLocalFlow;
import cascading.flow.Flow;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class MovieDistances {

	public static Pipe makePipe() {
		Pipe pipe = new Pipe("movieReviews");

		// line is tab delimited
		pipe = new Each(pipe, new Fields("line"), new RegexSplitter("\t"));

		return pipe;
	}

	public static void main(String[] args) throws Exception {
		Flow flow = makeLocalFlow(args[0], args[1] + "/long", makePipe());

		flow.complete();
	}

}
