package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class JsonLoadFunc extends LoadFunc {

	private final String[] m_fieldNames;
	private RecordReader<LongWritable, Text> m_in;
	private final TupleFactory mTupleFactory = TupleFactory.getInstance();
	private final JsonFactory m_jsonFactory;

	public JsonLoadFunc(String... pathAndFields) throws IOException {
		if (pathAndFields.length == 0) {
			throw new IllegalArgumentException("Must have at least one arg, the subpath");
		}
		m_fieldNames = pathAndFields;
		m_jsonFactory = new JsonFactory();
	}

	@Override
	public void setLocation(String p_location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, new Path(p_location));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader p_reader, PigSplit p_split) throws IOException {
		m_in = p_reader;
	}

	/**
	 * Reads in the JSON formatted string, looking for the fields in
	 * {@link #m_fieldNames}
	 * 
	 * @param bytes
	 * @return
	 * @throws JsonParseException
	 * @throws IOException
	 */
	protected String[] getFieldsInOrder(byte[] bytes) throws JsonParseException, IOException {
		JsonParser parser = m_jsonFactory.createJsonParser(bytes);
		parser.nextToken();
		String[] ret = new String[m_fieldNames.length];
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = parser.getCurrentName();
			for (int i = 0; i < m_fieldNames.length; i++) {
				if (fieldName.equals(m_fieldNames[i])) {
					ret[i] = parser.getText();
					break;
				}
			}
		}
		return ret;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			boolean notDone = m_in.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Tuple t = mTupleFactory.newTuple(m_fieldNames.length);
			int fieldPos = 0;
			for (String fieldValue : getFieldsInOrder(((Text) m_in.getCurrentValue()).getBytes())) {
				t.set(fieldPos++, fieldValue);
			}
			return t;
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}

	}

}