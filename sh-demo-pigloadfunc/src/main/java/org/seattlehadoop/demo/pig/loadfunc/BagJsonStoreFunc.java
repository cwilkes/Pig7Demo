package org.seattlehadoop.demo.pig.loadfunc;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class BagJsonStoreFunc extends StoreFunc {

	private RecordWriter<Text, Text> m_writer;
	private final CharArrayWriter m_charArrayWriter = new CharArrayWriter();
	private final String m_keyFieldName;
	private final String[] m_fieldNames;
	private final JsonFactory m_jsonFactory;
	private final String m_bagName;

	/**
	 * User passes in '' to signify this field is skippped
	 * 
	 * @param fieldNames
	 */
	public BagJsonStoreFunc(String... fieldNames) {
		int pos = 0;
		m_keyFieldName = fieldNames[pos++];
		m_bagName = fieldNames[pos++];
		m_fieldNames = new String[fieldNames.length - pos];
		for (int i = 0; i < m_fieldNames.length; i++) {
			if (!fieldNames[pos].isEmpty()) {
				m_fieldNames[i] = fieldNames[pos];
			}
			pos++;
		}
		m_jsonFactory = new JsonFactory();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new TextOutputFormat<Text, Text>();
	}

	@Override
	public void setStoreLocation(String p_location, Job p_job) throws IOException {
		FileOutputFormat.setOutputPath(p_job, new Path(p_location));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToWrite(RecordWriter p_writer) throws IOException {
		m_writer = p_writer;
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		m_charArrayWriter.reset();
		JsonGenerator gen = m_jsonFactory.createJsonGenerator(m_charArrayWriter);
		gen.writeStartObject();
		gen.writeFieldName(m_keyFieldName);
		write(gen, t.get(0));
		gen.writeArrayFieldStart(m_bagName);
		for (Iterator<Tuple> it = ((DataBag) t.get(1)).iterator(); it.hasNext();) {
			Tuple t2 = it.next();
			gen.writeStartObject();
			for (int posInBag = 0; posInBag < m_fieldNames.length; posInBag++) {
				String fieldName = m_fieldNames[posInBag];
				if (fieldName != null) {
					gen.writeFieldName(fieldName);
					write(gen, t2.get(posInBag));
				}
			}
			gen.writeEndObject();
		}
		gen.writeEndArray();
		gen.writeEndObject();
		gen.flush();
		try {
			m_writer.write(new Text(m_charArrayWriter.toString()), null);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void write(JsonGenerator p_gen, Object p_object) throws JsonGenerationException, IOException {
		if (p_object == null) {
			p_gen.writeNull();
		} else if (p_object instanceof Long) {
			p_gen.writeNumber((Long) p_object);
		} else if (p_object instanceof Float) {
			p_gen.writeNumber((Float) p_object);
		} else {
			p_gen.writeString(p_object.toString());
		}
	}
}
