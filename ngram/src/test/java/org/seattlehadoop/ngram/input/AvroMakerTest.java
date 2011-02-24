package org.seattlehadoop.ngram.input;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.seattlehadoop.ngram.avro.TokenCount;

import com.google.common.io.Files;

public class AvroMakerTest {

	@Test
	public void testMainMethod() throws IOException {
		String outputDir = "target/test-output/avro";
		File outDir = new File(outputDir);
		if (outDir.exists())
			Files.deleteRecursively(outDir.getCanonicalFile());
		List<String> args = new ArrayList<String>();
		args.add("-input");
		args.add("src/test/resources/");
		args.add("-ext");
		args.add(".csv");
		args.add("-output");
		args.add(outputDir);
		args.add("-maxsize");
		args.add("1");
		AvroMaker.main(args.toArray(new String[args.size()]));
		assertEquals(1, outDir.list().length);
	}

	@Test
	public void testSkipBadInput() throws IOException {
		CloseableIterator<TokenCount> it = new AvroMaker(Arrays.asList(new File("src/test/resources/tabbedInput1.csv"), new File(
				"src/test/resources/badInput.csv")), null, null, 1L).readInputs();
		assertTrue(it.hasNext());
		it.next();
		assertTrue(it.hasNext());
		it.next();
		assertFalse(it.hasNext());
	}

	@Test
	public void testReadOneFile() throws IOException {
		CloseableIterator<TokenCount> it = AvroMaker.readFiles(Arrays.asList(new File("src/test/resources/tabbedInput1.csv")).iterator());

		assertTrue(it.hasNext());
		TokenCount tc = it.next();
		assertEquals("circumvallate", tc.token.toString());
		assertEquals(1978, tc.year);
		assertEquals(313, tc.matchCount);
		assertEquals(215, tc.pageCount);
		assertEquals(85, tc.volumeCount);

		assertTrue(it.hasNext());
		tc = it.next();
		assertEquals("circumvallate", tc.token.toString());
		assertEquals(1979, tc.year);
		assertEquals(183, tc.matchCount);
		assertEquals(147, tc.pageCount);
		assertEquals(77, tc.volumeCount);

		assertFalse(it.hasNext());
	}
}
