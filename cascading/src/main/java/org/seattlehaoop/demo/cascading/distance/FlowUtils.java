package org.seattlehaoop.demo.cascading.distance;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;

public class FlowUtils {

	public static Flow makeHDFSFlow(String sourceFile, String sinkFile, Pipe pipe) {
		Tap source = new Hfs(new TextLine(), sourceFile);
		Tap sink = new Hfs(new TextLine(), sinkFile, true);
		return makeFlow(source, sink, pipe);
	}

	public static Flow makeLocalFlow(String sourceFile, String sinkFile, Pipe pipe) {
		Tap source = new Lfs(new TextLine(), sourceFile);
		Tap sink = new Lfs(new TextLine(), sinkFile, true);
		return makeFlow(source, sink, pipe);
	}

	public static Flow makeFlow(Tap source, Tap sink, Pipe pipe) {
		return makeFlowConnector().connect(source, sink, pipe);
	}

	private static FlowConnector makeFlowConnector() {
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, MovieDistances.class);
		return new FlowConnector(properties);
	}
}
