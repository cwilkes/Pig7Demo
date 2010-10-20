package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

public class ImageStoreFunc extends StoreFunc {

	private RecordWriter<Text, BytesWritable> m_writer;
	private BytesWritable m_bytesWritable;

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new SequenceFileOutputFormat<Text, BytesWritable>();
	}

	@Override
	public void setStoreLocation(String p_location, Job p_job) throws IOException {
		SequenceFileOutputFormat.setOutputPath(p_job, new Path(p_location));
		p_job.setOutputKeyClass(Text.class);
		p_job.setOutputValueClass(BytesWritable.class);		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToWrite(RecordWriter p_writer) throws IOException {
		m_writer = p_writer;
		m_bytesWritable = new BytesWritable();
	}

	@Override
	public void putNext(Tuple p_t) throws IOException {
		String fileName = (String) p_t.get(0);
		byte[] bytes = (byte[]) p_t.get(1);
		try {
			System.out.println("Writing out " + fileName + " of size " + bytes.length);
			m_bytesWritable.set(bytes, 0, bytes.length);			
			m_writer.write(new Text(fileName), m_bytesWritable);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
