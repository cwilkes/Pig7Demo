package org.seattlehaoop.demo.cascading.wordcount;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

public class ShingleFunctionTest extends CascadingTestCase {

	public void testMakeShingles() {
		Fields resultFields = Fields.size(1);
		TupleListCollector collector = invokeFunction(new ShingleFunction(new Fields("token"), 2), new Tuple("A Bb C"), resultFields);
		Iterator<TupleEntry> it = collector.entryIterator();
		assertEquals("A", it.next().getString(0));
		assertEquals("A Bb", it.next().getString(0));
		assertEquals("Bb", it.next().getString(0));
		assertEquals("Bb C", it.next().getString(0));
		assertEquals("C", it.next().getString(0));
		assertFalse(it.hasNext());
	}
}
