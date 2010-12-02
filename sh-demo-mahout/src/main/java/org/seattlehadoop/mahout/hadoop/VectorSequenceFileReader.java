package org.seattlehadoop.mahout.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

public class VectorSequenceFileReader implements Iterator<Pair<String, double[]>> {

	private final TaskAttemptContext m_taskAttempContext;
	private final RecordReader<Text, VectorWritable> m_createRecordReader;
	private String m_key;
	private double[] m_value;

	public VectorSequenceFileReader(File input) throws IOException, InterruptedException {
		SequenceFileInputFormat<Text, VectorWritable> inputFormat = new SequenceFileInputFormat<Text, VectorWritable>();
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
		m_key = m_createRecordReader.getCurrentKey().toString();
		DenseVector vector = (DenseVector) (m_createRecordReader.getCurrentValue().get());
		m_value = new double[vector.getNumNondefaultElements()];
		for (int i = 0; i < m_value.length; i++) {
			m_value[i] = vector.get(i);
		}
	}

	@Override
	public boolean hasNext() {
		return m_key != null;
	}

	@Override
	public Pair<String, double[]> next() {
		Pair<String, double[]> ret = new Pair<String, double[]>(m_key, m_value);
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
