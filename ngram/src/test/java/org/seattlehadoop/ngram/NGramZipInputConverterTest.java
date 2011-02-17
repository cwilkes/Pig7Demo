package org.seattlehadoop.ngram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.seattlehadoop.ngram.input.NGramZipInputConverter.writeOut;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.seattlehadoop.ngram.input.TokenAndCounts;

import com.google.common.io.Files;

public class NGramZipInputConverterTest {

	@Test
	public void testNoEntriesToWrite() throws FileNotFoundException, IOException, InterruptedException {
		File tmpDir = Files.createTempDir();
		List<TokenAndCounts> tokens = new ArrayList<TokenAndCounts>();
		writeOut(tokens.iterator(), tmpDir, 1);
		assertEquals(0, tmpDir.list().length);
	}

	@Test
	public void testMultiEntriesToWrite() throws FileNotFoundException, IOException, InterruptedException {
		File tmpDir = Files.createTempDir();
		List<TokenAndCounts> tokens = new ArrayList<TokenAndCounts>();
		tokens.add(new TokenAndCounts("circumvallate", 1978, 313, 215, 85));
		tokens.add(new TokenAndCounts("a", 1, 2, 3, 4));
		writeOut(tokens.iterator(), tmpDir, 1);
		String[] names = tmpDir.list();
		assertEquals(4, names.length);
		Arrays.sort(names);
		System.out.println(Arrays.toString(names));
		String ngram1 = names[0].substring(1).replaceFirst(".crc$", "");
		assertTrue(new File(tmpDir, ngram1).isFile());
		String ngram2  = names[1].substring(1).replaceFirst(".crc$", "");
		assertTrue(new File(tmpDir, ngram2).isFile());
	}
}
