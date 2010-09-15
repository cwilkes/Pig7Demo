package org.seattlehadoop.demo.pig.loadfunc;

import java.io.IOException;
import java.util.Arrays;

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
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class JsonLoadFunc extends LoadFunc {

	private final String[] m_fieldNames;
	private final String m_subPath;
	private RecordReader<LongWritable, Text> m_in;
	private final TupleFactory mTupleFactory = TupleFactory.getInstance();
	private final JsonFactory m_jsonFactory;

	public JsonLoadFunc(String... pathAndFields) throws IOException {
		if (pathAndFields.length == 0) {
			throw new IllegalArgumentException("Must have at least one arg, the subpath");
		}
		m_subPath = pathAndFields[0];
		m_fieldNames = Arrays.copyOfRange(pathAndFields, 1, pathAndFields.length - 1);
		m_jsonFactory = new JsonFactory();
	}

	public void setLocation(String p_location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, new Path(p_location, m_subPath));
	}

	@SuppressWarnings("rawtypes")
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@SuppressWarnings("unchecked")
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader p_reader, PigSplit p_split) throws IOException {
		m_in = p_reader;
	}

	public Tuple getNext() throws IOException {
		try {
			boolean notDone = m_in.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) m_in.getCurrentValue();
			JsonParser parser = m_jsonFactory.createJsonParser(value.getBytes());
			parser.nextToken();
			for (String field : m_fieldNames) {
				if (field.equals(parser.getCurrentName())) {
					
				}
			}
			JsonToken token = parser.nextToken();
			
			byte[] buf = value.getBytes();
			int len = value.getLength();
			int start = 0;

			for (int i = 0; i < len; i++) {
				if (buf[i] == fieldDel) {
					readField(buf, start, i);
					start = i + 1;
				}
			}
			// pick up the last field
			readField(buf, start, len);

			Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
			mProtoTuple = null;
			return t;
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}

	}
}
