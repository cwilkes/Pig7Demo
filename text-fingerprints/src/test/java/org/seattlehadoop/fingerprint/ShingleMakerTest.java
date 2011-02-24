package org.seattlehadoop.fingerprint;

import static org.seattlehadoop.fingerprint.LineReaderTest.getMobyDick;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.seattlehadoop.fingerprint.reader.ShingleMaker;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class ShingleMakerTest {

	private Iterator<String> m_mobyDick;

	@Before
	public void setup() {
		m_mobyDick = getMobyDick();
	}

	@After
	public void teardown() throws IOException {
		while (m_mobyDick.hasNext()) {
			m_mobyDick.next();
		}
	}

	@Test
	public void testMobyDickShingles() throws IOException {
		Iterator<String> lines = new ShingleMaker(m_mobyDick, 3);
		final Multiset<String> counts = HashMultiset.create();
		while (lines.hasNext()) {
			counts.add(lines.next());
		}

		List<String> words = new ArrayList<String>(counts.elementSet());
		Collections.sort(words, new Comparator<String>() {

			@Override
			public int compare(String p_o1, String p_o2) {
				int c1 = counts.count(p_o1);
				int c2 = counts.count(p_o2);
				return c2 - c1;
			}
		});
		int i = 0;
		for (String word : words) {
			System.out.println(word + " " + counts.count(word));
			if (i++ > 50)
				break;
		}
	}
}
