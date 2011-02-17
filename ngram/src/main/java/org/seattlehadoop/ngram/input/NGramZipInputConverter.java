package org.seattlehadoop.ngram.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.common.io.Files;

public class NGramZipInputConverter {

	private static void moveWriterOutput(File outDir, File lastOutFile) throws IOException {
		int id = Integer.parseInt(lastOutFile.getName().split("-")[2]);
		String newFileName = outDir.getName().replaceFirst(".dir$", String.format("-%d%s", id, new BZip2Codec().getDefaultExtension()));
		Files.move(lastOutFile, new File(outDir.getParentFile(), newFileName));
		Files.move(new File(lastOutFile.getParentFile(), "." + lastOutFile.getName() + ".crc"), new File(outDir.getParentFile(), "." + newFileName + ".crc"));
	}

	public static void writeOut(Iterator<TokenAndCounts> in, File outputDir, int maxEntries) throws FileNotFoundException, IOException, InterruptedException {
		int i = 0;
		RecordWriter<Text, CountsArray> recordWriter = null;
		TaskAttemptContext context = null;
		int jobId = 10;
		int id = 0;
		long totalOutput = 0;
		long totalTokens = 0;
		File lastOutFile = null;
		File outDir = File.createTempFile("ngram-", ".dir", outputDir);
		while (in.hasNext()) {
			if (i++ > maxEntries) {
				System.err.println(String.format("Closing outputstream at %d as have %d entries.  Total tokens: %d, Total lines: %d",
						System.currentTimeMillis(), i, totalTokens, totalOutput));
				recordWriter.close(context);
				moveWriterOutput(outDir, lastOutFile);
				recordWriter = null;
				i = 0;
			}
			if (recordWriter == null) {
				SequenceFileOutputFormat<Text, CountsArray> seq = new SequenceFileOutputFormat<Text, CountsArray>();
				Job job = new Job(new Configuration());
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(CountsArray.class);
				SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
				SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
				lastOutFile = new File(outDir, String.format("_temporary/_attempt_none_%04d_r_%06d_%d/part-r-%05d", jobId, id, id, id));
				System.err.println("Writing to " + lastOutFile);
				outDir.delete();
				SequenceFileOutputFormat.setOutputPath(job, new Path(outDir.getAbsolutePath()));
				context = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID(new TaskID("none", jobId++, false, id), id));
				id++;
				recordWriter = seq.getRecordWriter(context);
			}
			TokenAndCounts tc = in.next();
			recordWriter.write(new Text(tc.getToken()), new CountsArray(tc.getCounts()));
			totalTokens++;
			totalOutput += tc.getCounts().size();
		}
		if (recordWriter != null) {
			moveWriterOutput(outDir, lastOutFile);
			recordWriter.close(context);
		}
		Files.deleteRecursively(outDir.getCanonicalFile());
		System.err.println(String.format("Done.  Total tokens: %d, Total lines: %d", totalTokens, totalOutput));
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		int pos = 0;
		final File in = new File(args[pos++]);
		if (!in.isFile()) {
			System.err.println("File " + in + " does not exist");
			System.exit(1);
		}
		File outDir = new File(args[pos++]);
		outDir.mkdirs();
		if (!outDir.isDirectory()) {
			System.err.println("Out directory " + outDir + " does not exist");
			System.exit(1);
		}
		int numberTokensPerFile = Integer.parseInt(args[pos++]);	
		System.err.println("Reading in " + in.getAbsolutePath());
		writeOut(new RawNGramReader(in, 1900, 2000), outDir, numberTokensPerFile);
	}
}
