package org.seattlehadoop.mahout.hadoop;

import static uk.co.flamingpenguin.jewel.cli.CliFactory.parseArguments;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.mahout.common.Pair;
import org.seattlehadoop.mahout.Utils;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;
import uk.co.flamingpenguin.jewel.cli.CliFactory;
import uk.co.flamingpenguin.jewel.cli.Option;

import com.google.common.base.Function;
import com.google.common.primitives.Doubles;

public class TabDelimitedSequenceFileWriterCLI {

	private static final Pattern CONVERTER_PAIR_CLASSNAME = Pattern.compile("^.*org.apache.mahout.common.Pair<(.*), (.*?)>>$");

	public static interface TabDelimitedSequenceFileWriterOptions {

		@Option(longName = "keepheader")
		boolean keepheader();

		@Option(shortName = "r")
		boolean isReading();

		@Option(shortName = "c", defaultValue = "none")
		String getLineConverter();

		@Option(shortName = "f")
		String getInputFile();

		@Option(shortName = "o", defaultValue = "none")
		String getOutputDirectory();
	}

	private static TabDelimitedSequenceFileWriterOptions parseCommandLine(String[] p_args) {
		try {
			return parseArguments(TabDelimitedSequenceFileWriterOptions.class, p_args);
		} catch (ArgumentValidationException e) {
			System.err.println(e.getMessage());
			System.exit(1);
			return null;
		}
	}

	public static void printOutSequenceFile(File file) throws IOException, InterruptedException {
		Iterator<Pair<String, double[]>> it = new VectorSequenceFileReader(file);
		while (it.hasNext()) {
			Pair<String, double[]> pair = it.next();
			System.out.println(pair.getFirst() + Utils.TAB + Doubles.join(" ", pair.getSecond()));
		}
	}

	private static void checkForNonReading(TabDelimitedSequenceFileWriterOptions parser) {
		if (parser.getLineConverter().equals("none") || parser.getOutputDirectory().equals("none")) {
			System.out.println(CliFactory.createCli(TabDelimitedSequenceFileWriterOptions.class).getHelpMessage());
			System.exit(1);
		}
	}

	private static class GenericFixer {

		private final Function<String, Pair<Object, Object>> m_lineConverter;
		@SuppressWarnings("rawtypes")
		private final Class[] m_converterTypes;

		@SuppressWarnings("unchecked")
		public GenericFixer(String lineConverterClass) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
			m_lineConverter = (Function<String, Pair<Object, Object>>) Class.forName(lineConverterClass).newInstance();

			String asString = m_lineConverter.getClass().getGenericInterfaces()[0].toString();
			Matcher m = CONVERTER_PAIR_CLASSNAME.matcher(asString);
			if (m.matches()) {
				m_converterTypes = new Class[] { Class.forName(m.group(1)), Class.forName(m.group(2)) };
			} else {
				throw new IllegalStateException("Cannot find pair in " + asString);
			}
			System.err.println("Converter types: " + Arrays.toString(getConverterTypes()));
		}

		@SuppressWarnings("rawtypes")
		public Class[] getConverterTypes() throws ClassNotFoundException {
			return m_converterTypes;
		}

		@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
		public boolean isPairValueClass(Class desiredValue) throws ClassNotFoundException {
			return getConverterTypes()[1].isAssignableFrom(desiredValue);
		}

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws SecurityException, IOException, InterruptedException, NoSuchMethodException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		TabDelimitedSequenceFileWriterOptions parser = parseCommandLine(args);
		if (parser.isReading()) {
			printOutSequenceFile(new File(parser.getInputFile()));
			System.exit(0);
		}
		checkForNonReading(parser);
		GenericFixer fixer = new GenericFixer(parser.getLineConverter());
		File outputDir = new File(parser.getOutputDirectory());
		outputDir.mkdirs();
		createDataSequenceFile(fixer.m_lineConverter, fixer.getConverterTypes()[0], fixer.getConverterTypes()[1], new File(parser.getInputFile()),
				!parser.keepheader(), outputDir);
	}

	public static <K, V> void createDataSequenceFile(Function<String, Pair<K, V>> lineConverter, Class<K> keyType, Class<V> valueType, File inputFile,
			boolean skipHeaderLine, File outputDirectory) throws SecurityException, IOException, InterruptedException, NoSuchMethodException,
			IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		TabDelimitedSequenceFileWriter<K, V> writer = new TabDelimitedSequenceFileWriter<K, V>(keyType, valueType, lineConverter);
		Reader reader = new FileReader(inputFile);
		File outFile = File.createTempFile("part-" + valueType.getSimpleName() + "-", ".seq", outputDirectory);
		writer.process(reader, skipHeaderLine, outFile);
		reader.close();
		System.out.println("Output for " + keyType.getName() + "," + valueType.getName() + " is : " + outFile);

	}
}
