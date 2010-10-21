package org.seattlehaoop.demo.cascading.wordcount;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import org.junit.Test;

import cascading.tuple.Tuple;

public class WordCountTest {

	@Test
	public void testTokenizer() {
		MockFunctionCall<Matcher> functionCall = new MockFunctionCall<Matcher>().setArgumentString("the quick brown fox");
	//	MockFlowProcess flowProcess = new MockFlowProcess();
	//	WordCount.wordTokenizer.prepare(flowProcess, functionCall);
	//	WordCount.wordTokenizer.operate(flowProcess, functionCall);
		List<String> words = new ArrayList<String>();
		for (Tuple t : functionCall.getTuples()) {
			assertEquals(1, t.size());
			words.add(t.getString(0));

		}
		assertArrayEquals(new String[] { "the", "quick", "brown", "fox" }, words.toArray(new String[0]));
	}
}
