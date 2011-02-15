package org.seattlehadoop.mahout.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.mahout.common.Pair;

public class SequenceFileReader<KEY, VALUE> implements Iterator<Pair<KEY, VALUE>> {

	private final TaskAttemptContext m_taskAttempContext;
	private final RecordReader<KEY, VALUE> m_createRecordReader;
	private KEY m_key;
	private VALUE m_value;

	public SequenceFileReader(File input, Class<KEY> keyClass, Class<VALUE> valueClass) throws IOException, InterruptedException {
		SequenceFileInputFormat<KEY, VALUE> inputFormat = new SequenceFileInputFormat<KEY, VALUE>();
		Job job = new Job(new Configuration());
		m_taskAttempContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID("test", 7, false, 8, 2));
		InputSplit in = new FileSplit(new Path(input.getAbsolutePath()), 0, input.length(), null);
		m_createRecordReader = inputFormat.createRecordReader(in, m_taskAttempContext);
		m_createRecordReader.initialize(in, m_taskAttempContext);
		advance();
	}

	private void advance() throws IOException, InterruptedException {
		if (!m_createRecordReader.nextKeyValue()) {
			m_key = null;
			m_value = null;
			m_createRecordReader.close();
			return;
		}
		m_key = m_createRecordReader.getCurrentKey();
		m_value = m_createRecordReader.getCurrentValue();
	}

	@Override
	public boolean hasNext() {
		return m_key != null;
	}

	@Override
	public Pair<KEY, VALUE> next() {
		Pair<KEY, VALUE> ret = new Pair<KEY, VALUE>(m_key, m_value);
		try {
			advance();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
