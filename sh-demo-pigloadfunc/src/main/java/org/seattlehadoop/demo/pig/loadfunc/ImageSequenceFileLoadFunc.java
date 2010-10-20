package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSequenceFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ImageSequenceFileLoadFunc extends LoadFunc {

	private RecordReader<Text, BytesWritable> m_reader;
	private final TupleFactory mTupleFactory = TupleFactory.getInstance();

	@Override
	public void setLocation(String p_location, Job p_job) throws IOException {
		PigSequenceFileInputFormat.setInputPaths(p_job, new Path(p_location));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new PigSequenceFileInputFormat<Text, BytesWritable>();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToRead(RecordReader p_reader, PigSplit p_split) throws IOException {
		m_reader = p_reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			if (!m_reader.nextKeyValue()) {
				return null;
			}
			String fileName = m_reader.getCurrentKey().toString();
			BytesWritable bw = m_reader.getCurrentValue();
			Tuple t = mTupleFactory.newTuple(2);
			t.set(0, fileName);
			t.set(1, Arrays.copyOf(bw.getBytes(), bw.getLength()));
			return t;
		} catch (InterruptedException ex) {
			ex.printStackTrace();
			return null;
		}
	}

}
