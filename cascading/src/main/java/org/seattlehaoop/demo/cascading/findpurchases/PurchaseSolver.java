package org.seattlehaoop.demo.cascading.findpurchases;

import static org.seattlehaoop.demo.cascading.findpurchases.GenerateLogs.DATE_FORMAT;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Filter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
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

public class PurchaseSolver {

	private static final String FIELD_WORD = "word";
	private static final String FIELD_COUNT = "count";
	private static final String FIELD_LINE = "line";

	private static final String FIELD_DATE = "date";
	private static final String FIELD_USERID = "userid";
	private static final String FIELD_MOVEMENT = "movement";

	private static String movementRegex = "(\\S+)\\s+STATUS\\s+(\\d+)\\s+(ENTER|LEAVE)";
	private static Fields movementFields = new Fields(FIELD_DATE, FIELD_USERID, FIELD_MOVEMENT);
	protected static Filter<Matcher> onlyMovementLines = new RegexFilter(movementRegex);
	protected static RegexParser movementParser = new RegexParser(movementFields, movementRegex, new int[] { 1, 2, 3 });

	private static DateParser parser = new DateParser(new Fields(FIELD_DATE), DATE_FORMAT);

	public static Flow makeFlow(String source, String sink, boolean useHDFS) {
		Scheme sourceScheme = new TextLine(new Fields(FIELD_LINE));
		Scheme sinkScheme = new TextLine();
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
		FlowConnector.setApplicationJarClass(properties, PurchaseSolver.class);
		return new FlowConnector(properties);
	}

	private static Pipe makePipe() {
		Pipe assembly = new Pipe("wordcount");
		assembly = new Each(assembly, new Fields(FIELD_LINE), onlyMovementLines);
		assembly = new Each(assembly, new Fields(FIELD_LINE), movementParser);
		assembly = new GroupBy(assembly, new Fields(FIELD_USERID));
		//assembly = new Every(assembly, new Fields(FIELD_USERID));
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
