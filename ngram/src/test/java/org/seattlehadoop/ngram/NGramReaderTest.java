package org.seattlehadoop.ngram;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.junit.Test;

public class NGramReaderTest {

	@Test
	public void testOneLine() throws IOException {
		Iterator<TokenAndCounts> it = new RawNGramReader(new StringReader("circumvallate\t1978\t313\t215\t85"));
		assertTrue(it.hasNext());
		TokenAndCounts tc = it.next();
		assertEquals("circumvallate", tc.getToken());
		assertEquals(1, tc.getCounts().size());
		int[] entry = tc.getCounts().get(0);
		int pos = 0;
		assertEquals(1978, entry[pos++]);
		assertEquals(313, entry[pos++]);
		assertEquals(215, entry[pos++]);
		assertEquals(85, entry[pos++]);
		assertFalse(it.hasNext());
	}

	@Test
	public void testTwoLines() throws IOException {
		Iterator<TokenAndCounts> it = new RawNGramReader(new StringReader("circumvallate\t1978\t313\t215\t85\ncircumvallate\t1979\t183\t147\t77"));
		assertTrue(it.hasNext());
		TokenAndCounts tc = it.next();
		assertEquals("circumvallate", tc.getToken());
		assertEquals(2, tc.getCounts().size());
		int[] entry = tc.getCounts().get(0);
		int pos = 0;
		assertEquals(1978, entry[pos++]);
		assertEquals(313, entry[pos++]);
		assertEquals(215, entry[pos++]);
		assertEquals(85, entry[pos++]);
		entry = tc.getCounts().get(1);
		pos = 0;
		assertEquals(1979, entry[pos++]);
		assertEquals(183, entry[pos++]);
		assertEquals(147, entry[pos++]);
		assertEquals(77, entry[pos++]);
		assertFalse(it.hasNext());
	}

	@Test
	public void testTwoTokens() throws IOException {
		Iterator<TokenAndCounts> it = new RawNGramReader(new StringReader("circumvallate\t1978\t313\t215\t85\ncircumvallate\t1979\t183\t147\t77\na\t1\t2\t3\t4"));
		assertTrue(it.hasNext());
		TokenAndCounts tc = it.next();
		System.out.println(tc);
		assertTrue(it.hasNext());
		tc = it.next();
		System.out.println(tc);
		assertEquals("a", tc.getToken());
		assertEquals(1, tc.getCounts().size());
		int[] entry = tc.getCounts().get(0);
		int pos = 0;
		assertEquals(1, entry[pos++]);
		assertEquals(2, entry[pos++]);
		assertEquals(3, entry[pos++]);
		assertEquals(4, entry[pos++]);
		if (it.hasNext()) {
			fail("Should not have " + it.next());
		}
	}
}
