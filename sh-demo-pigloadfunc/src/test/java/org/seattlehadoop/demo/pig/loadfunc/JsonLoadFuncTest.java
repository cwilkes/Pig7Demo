package org.seattlehadoop.demo.pig.loadfunc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class JsonLoadFuncTest {

	protected boolean canBeUnQuotes(String val) {
		return val.matches("\\d+") || val.equals("true") || val.equals("false");
	}

	protected String makeJSONLine(String line) {
		String[] fieldsAndValues = line.split("\\s+");
		StringBuffer sb = new StringBuffer("{ ");
		for (int i = 0; i < fieldsAndValues.length; i += 2) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append("\"" + fieldsAndValues[i] + "\":");
			if (canBeUnQuotes(fieldsAndValues[i + 1])) {
				sb.append(fieldsAndValues[i + 1]);
			} else {
				sb.append("\"" + fieldsAndValues[i + 1] + "\"");
			}
		}
		sb.append(" }");
		return sb.toString();
	}

	@Test
	public void testParseSimpleJson() throws IOException, InterruptedException {
		LoadFunc loadFunc = new JsonLoadFunc("a", "c", "b");
		@SuppressWarnings("unchecked")
		RecordReader<LongWritable, Text> reader = createMock(RecordReader.class);
		expect(reader.nextKeyValue()).andReturn(true);
		Text text = new Text(makeJSONLine("d ignore a 1 b hi c true"));
		System.out.println("JSON input: " + text);
		expect(reader.getCurrentValue()).andReturn(text);
		replay(reader);
		loadFunc.prepareToRead(reader, null);
		Tuple t = loadFunc.getNext();
		System.out.println(t);
		int i = 0;
		for (String expected : new String[] { "1", "true", "hi" }) {
			assertEquals(expected, (String) t.get(i++));
		}
	}
}
