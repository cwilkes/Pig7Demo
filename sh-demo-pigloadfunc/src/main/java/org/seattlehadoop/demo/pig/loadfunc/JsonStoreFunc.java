package org.seattlehadoop.demo.pig.loadfunc;

import java.io.CharArrayWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

public class JsonStoreFunc extends StoreFunc {

	private final String[] m_fieldNames;
	private final JsonFactory m_jsonFactory;
	private RecordWriter<Text, Text> m_writer;
	private final CharArrayWriter m_charArrayWriter;

	public JsonStoreFunc(String... fieldNames) {
		m_fieldNames = fieldNames;
		m_jsonFactory = new JsonFactory();
		m_charArrayWriter = new CharArrayWriter();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, Text>();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter p_writer) throws IOException {
		m_writer = p_writer;
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		int pos = 0;
		m_charArrayWriter.reset();
		JsonGenerator gen = m_jsonFactory.createJsonGenerator(m_charArrayWriter);
		gen.writeStartObject();
		for (String fieldName : m_fieldNames) {
			Object val = t.get(pos++);
			if (val == null) {
				gen.writeNullField(fieldName);
			} else {
				gen.writeStringField(fieldName, val.toString());
			}			
		}
		gen.writeEndObject();
		gen.flush();
		try {
			m_writer.write(new Text(m_charArrayWriter.toString()), null);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
