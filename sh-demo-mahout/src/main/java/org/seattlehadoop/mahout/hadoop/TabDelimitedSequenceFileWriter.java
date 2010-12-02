package org.seattlehadoop.mahout.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.Pair;

import com.google.common.base.Function;

public class TabDelimitedSequenceFileWriter<KEY, VALUE> {

	private final SequenceFileOutputFormat<KEY, VALUE> m_output;
	private final RecordWriter<KEY, VALUE> m_recordWriter;
	private final TaskAttemptContext m_taskAttemptContext;
	private File m_tmpDir;
	private final Function<String, Pair<KEY, VALUE>> m_lineConverter;

	public TabDelimitedSequenceFileWriter(Class<KEY> keyClass, Class<VALUE> valueClass, Function<String, Pair<KEY, VALUE>> lineConverter) throws IOException,
			InterruptedException, SecurityException, NoSuchMethodException {
		m_lineConverter = lineConverter;
		m_output = new SequenceFileOutputFormat<KEY, VALUE>();
		Job job = new Job(new Configuration());
		job.setOutputKeyClass(keyClass);
		job.setOutputValueClass(valueClass);
		m_tmpDir = File.createTempFile(getClass().getName() + "-", ".tmp");
		m_tmpDir.delete();
		m_tmpDir.mkdirs();
		job.getConfiguration().set("mapred.output.dir", m_tmpDir.getAbsolutePath());
		m_taskAttemptContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID(new TaskID("demo", 0, false, 1), 2));
		m_recordWriter = m_output.getRecordWriter(m_taskAttemptContext);
	}

	public void process(Reader r, boolean skipHeader, File outputFile) throws IOException, IllegalArgumentException, InterruptedException,
			IllegalAccessException, InvocationTargetException {
		BufferedReader br = new BufferedReader(r);
		String s;
		if (skipHeader) {
			if (br.readLine() == null) {
				return;
			}
		}
		while ((s = br.readLine()) != null) {
			writeFromLine(s);
		}
		close(outputFile);
	}

	public void writeFromLine(String line) throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Pair<KEY, VALUE> keyAndValue = m_lineConverter.apply(line);
		write(keyAndValue.getFirst(), keyAndValue.getSecond());
	}

	public void write(KEY key, VALUE value) throws IOException, InterruptedException {
		m_recordWriter.write(key, value);
	}

	public void close(File outputFile) throws IOException, InterruptedException {
		m_recordWriter.close(m_taskAttemptContext);
		File writerOutputFile = new File(m_tmpDir, "_temporary/_attempt_demo_0000_r_000001_2/part-r-00001");
		FileUtil.replaceFile(writerOutputFile, outputFile);
	}
}
