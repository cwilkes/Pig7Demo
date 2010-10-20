package org.seattlehaoop.demo.cascading.wordcount;

import java.util.Arrays;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

public class WordCount {
	//private static final String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

	/**
	 * A word from a {@link #FIELD_LINE}
	 */
	private static final String FIELD_WORD = "word";
	/**
	 * The count of an individual word in the input
	 */
	private static final String FIELD_COUNT = "count";
	/**
	 * An individual line of text
	 */
	private static final String FIELD_LINE = "line";

	// protected static RegexGenerator wordTokenizer = new RegexGenerator(new
	// Fields(FIELD_WORD), regex);
	protected static ShingleFunction wordTokenizer = new ShingleFunction(new Fields(FIELD_WORD), 2, true);
	protected static ExpressionFunction lowerCaser = new ExpressionFunction(new Fields(FIELD_WORD), "$0.toLowerCase()", String.class);

	public static Flow makeFlow(String source, String sink, boolean useHDFS) {
		Scheme sourceScheme = new TextLine(new Fields(FIELD_LINE));
		Scheme sinkScheme = new TextLine(new Fields(FIELD_WORD, FIELD_COUNT));
		if (useHDFS) {
			return makeFlow(new Hfs(sourceScheme, source), new Hfs(sinkScheme, sink, SinkMode.REPLACE));
		} else {
			return makeFlow(new Lfs(sourceScheme, source), new Lfs(sinkScheme, sink, SinkMode.REPLACE));
		}
	}

	public static Flow makeFlow(Hfs source, Hfs sink) {
		FlowConnector flowConnector = makeFlowConnector();
		Pipe pipe = makePipe();
		return flowConnector.connect("word-count", source, sink, pipe);
	}

	private static FlowConnector makeFlowConnector() {
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, WordCount.class);
		return new FlowConnector(properties);
	}

	private static Pipe makePipe() {
		Pipe assembly = new Pipe("wordcount");
		assembly = new Each(assembly, new Fields(FIELD_LINE), wordTokenizer);
		assembly = new Each(assembly, new Fields(FIELD_WORD), lowerCaser);
		assembly = new Each(assembly, new Fields(FIELD_WORD), wordTokenizer);
		assembly = new GroupBy(assembly, new Fields(FIELD_WORD));
		assembly = new Every(assembly, new Count(new Fields(FIELD_COUNT)));
		return assembly;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Passed in " + Arrays.toString(args) + " need (inputPath) (outputPath)");
			System.exit(1);
		}
		int pos = 0;
		boolean useHDFS = true;
		if (args[pos].equals("-local")) {
			useHDFS = false;
			pos++;
		}
		Flow flow = makeFlow(args[pos++], args[pos++], useHDFS);
		flow.writeDOT(args[2]);
		flow.complete();
	}
}
