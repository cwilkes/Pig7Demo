package org.seattlehaoop.demo.cascading.wordcount;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class ShingleMakerTest {

	@Test
	public void testSplitSimpleLine() {
		String line = "1 2 3 4";
		Iterator<String> it = new ShingleMaker(2, false).makeShingles(line);
		assertEquals("1", it.next());
		assertEquals("1 2", it.next());
		assertEquals("2", it.next());
		assertEquals("2 3", it.next());
		assertEquals("3", it.next());
		assertEquals("3 4", it.next());
		assertEquals("4", it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testLineLessThanNumberSplits() {
		String line = "1 2 3";
		Iterator<String> it = new ShingleMaker(4, false).makeShingles(line);
		assertEquals("1", it.next());
		assertEquals("1 2", it.next());
		assertEquals("1 2 3", it.next());
		assertEquals("2", it.next());
		assertEquals("2 3", it.next());
		assertEquals("3", it.next());
		if (it.hasNext()) {
			fail("Shouldn't have " + Arrays.asList(it.next()));
		}
	}
	
	@Test
	public void testLineExactCountSplits() {
		String line = "1 2 3 4";
		Iterator<String> it = new ShingleMaker(2, true).makeShingles(line);		
		assertEquals("1 2", it.next());
		assertEquals("2 3", it.next());
		assertEquals("3 4", it.next());
		if (it.hasNext()) {
			fail("Shouldn't have " + Arrays.asList(it.next()));
		}
	}
	@Test
	public void testLineExactCountSplitsTooFew() {
		String line = "1 2 3 4";
		Iterator<String> it = new ShingleMaker(5, true).makeShingles(line);		
		if (it.hasNext()) {
			fail("Shouldn't have " + Arrays.asList(it.next()));
		}
	}
}
