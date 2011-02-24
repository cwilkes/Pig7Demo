package org.seattlehadoop.ngram.input;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.PatternFilenameFilter;

public class AvroMakerArgParser {

	private final Logger m_logger = LoggerFactory.getLogger(getClass());
	private CommandLine m_commandLine;
	private final List<File> m_input;
	private final File m_output;
	private final long m_maxFileSize;

	public AvroMakerArgParser(String[] args) {
		this(new Options(), args);
	}

	public AvroMakerArgParser(Options options, String[] args) {
		parseGeneralOptions(options, args);
		m_input = parseInputs();
		m_output = new File(m_commandLine.getOptionValue("output"));
		if (m_output.isFile()) {
			throw new IllegalStateException("Passed in an existing file, please delete first");
		}
		m_output.mkdirs();
		if (!m_output.isDirectory()) {
			throw new IllegalStateException("Cannot make directory: " + m_output);
		}
		long maxFileSizeTmp = (m_commandLine.hasOption("maxsize")) ? Long.parseLong(m_commandLine.getOptionValue("maxsize")) : 125;
		m_maxFileSize = maxFileSizeTmp * 1024 * 1024;
	}

	public List<File> getInput() {
		return m_input;
	}

	public File getOutput() {
		return m_output;
	}

	public long getMaxFileSize() {
		return m_maxFileSize;
	}

	private List<File> parseInputs() {
		String fileOrDirectory = m_commandLine.getOptionValue("input");
		if (fileOrDirectory == null) {
			throw new IllegalStateException("Missing -input in " + m_commandLine);
		}
		String ext = m_commandLine.getOptionValue("ext");
		File f = new File(fileOrDirectory);
		if (f.isFile()) {
			if (ext != null && !f.getName().equals(ext)) {
				throw new IllegalStateException("Passed in a file " + f + " but does not match the given extension " + ext);
			}
			return Arrays.asList(f);
		}
		if (!f.isDirectory()) {
			throw new IllegalStateException("Not a file nor a directory: " + f);
		}
		if (ext == null) {
			ext = ".zip";
		}
		File[] inputs = f.listFiles(new PatternFilenameFilter(".*" + ext + "$"));
		if (inputs.length == 0) {
			throw new IllegalStateException("No files under " + f + " end with " + ext);
		}
		Arrays.sort(inputs);
		return Arrays.asList(inputs);
	}

	/**
	 * Taken from hadoop's GenericOptionsParser
	 * 
	 * @param opts
	 * @param args
	 * @return
	 */
	private String[] parseGeneralOptions(Options opts, String[] args) {
		opts = buildGeneralOptions(opts);
		CommandLineParser parser = new GnuParser();
		try {
			m_commandLine = parser.parse(opts, args, true);
			return m_commandLine.getArgs();
		} catch (ParseException e) {
			m_logger.warn("options parsing failed: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("general options are: ", opts);
		}
		return args;
	}

	@SuppressWarnings("static-access")
	private static Options buildGeneralOptions(Options opts) {
		Option in = OptionBuilder.withArgName("input").hasArg().withDescription("single file or directory").create("input");
		Option out = OptionBuilder.withArgName("output").hasArg().withDescription("output directory").create("output");
		Option maxSize = OptionBuilder.withArgName("maxSize").hasArg().withDescription("maximum file size in MB").create("maxsize");
		Option extension = OptionBuilder.withArgName("ext").hasArg().withDescription("file extension if not .zip").create("ext");
		opts.addOption(in);
		opts.addOption(out);
		opts.addOption(maxSize);
		opts.addOption(extension);
		return opts;
	}
}
