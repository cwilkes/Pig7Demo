package org.seattlehadoop.fingerprint;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.seattlehadoop.fingerprint.reader.ParagraphMaker;

public class ParagraphMakerTest {

	private Iterator<String> makeParagraphs(String... lines) {
		return new ParagraphMaker(Arrays.asList(lines).iterator());
	}

	@Test
	public void testGroupLinesExceptWithSpacePrefix() {
		Iterator<String> lines = makeParagraphs("the quick brown", "fox jumped over", "  the lazy dog.", " line three", "continues");
		assertEquals("the quick brown fox jumped over", lines.next());
		assertEquals("the lazy dog.", lines.next());
		assertEquals("line three continues", lines.next());
		assertFalse(lines.hasNext());
	}
}
