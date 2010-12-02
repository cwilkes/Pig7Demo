package org.seattlehadoop.mahout.hadoop;

import static uk.co.flamingpenguin.jewel.cli.CliFactory.parseArguments;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.VectorWritable;
import org.seattlehadoop.mahout.Utils;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;
import uk.co.flamingpenguin.jewel.cli.CliFactory;
import uk.co.flamingpenguin.jewel.cli.Option;

import com.google.common.base.Function;
import com.google.common.primitives.Doubles;

public class TabDelimitedSequenceFileWriterCLI {

	public static interface TabDelimitedSequenceFileWriterOptions {

		@Option(shortName = "r")
		boolean isReading();

		@Option(shortName = "c", defaultValue = "none")
		String getLineConverter();

		@Option(shortName = "f")
		String getInputFile();

		@Option(shortName = "o", defaultValue = "none")
		String getOutputDirectory();
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws SecurityException, IOException, InterruptedException, NoSuchMethodException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		TabDelimitedSequenceFileWriterOptions parser = null;
		try {
			parser = parseArguments(TabDelimitedSequenceFileWriterOptions.class, args);
		} catch (ArgumentValidationException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		if (parser.isReading()) {
			Iterator<Pair<String, double[]>> it = new VectorSequenceFileReader(new File(parser.getInputFile()));
			while (it.hasNext()) {
				Pair<String, double[]> pair = it.next();
				System.out.println(pair.getFirst() + Utils.TAB + Doubles.join(" ", pair.getSecond()));
			}
			System.exit(0);
		}
		if (parser.getLineConverter().equals("none") || parser.getOutputDirectory().equals("none")) {
			System.out.println(CliFactory.createCli(TabDelimitedSequenceFileWriterOptions.class).getHelpMessage());
			System.exit(1);
		}
		TabDelimitedSequenceFileWriter writer = new TabDelimitedSequenceFileWriter((Function<String, Pair<Text, VectorWritable>>) Class.forName(
				parser.getLineConverter()).newInstance());
		Reader reader = new FileReader(new File(parser.getInputFile()));
		File outFile = File.createTempFile("vectors-", ".seq", new File(parser.getOutputDirectory()));
		writer.process(reader, true, outFile);
		reader.close();
		System.out.println("Output is: " + outFile);
	}
}
