package org.seattlehadoop.fingerprint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;
import org.seattlehadoop.fingerprint.reader.LineReader;
import org.seattlehadoop.fingerprint.reader.ParagraphMaker;

public class LineReaderTest {

	private static final File m_inputDirectory = new File("src/test/resources");
	private static final String m_textFileName = "testlines.txt";
	private static final String m_gzipName = m_textFileName + ".gz";
	private static final String m_zipName = m_textFileName + ".zip";
	private static final String m_mobyDick = "book1_pg2701.txt";

	public static Iterator<String> getMobyDick() {
		try {
			return new ParagraphMaker(readFile(m_mobyDick + ".zip"));
		} catch (IOException e) {
			return new ArrayList<String>().iterator();
		}
	}

	private static LineReader readFile(String name) throws FileNotFoundException, IOException {
		return LineReader.readFile(new File(m_inputDirectory, name));
	}

	private void testLines(LineReader lines) throws IOException {
		try {
			assertEquals("\"The larger whales, they seldom venture to attack. They stand in so", lines.next());
			assertEquals("great dread of some of them, that when out at sea they are afraid to", lines.next());
			assertEquals("mention even their names, and carry dung, lime-stone, juniper-wood,", lines.next());
			assertEquals("and some other articles of the same nature in their boats, in order to", lines.next());
			assertEquals("terrify and prevent their too near approach.\" --UNO VON TROIL'S LETTERS", lines.next());
			assertEquals("ON BANKS'S AND SOLANDER'S VOYAGE TO ICELAND IN 1772.", lines.next());
			assertFalse(lines.hasNext());
		} finally {
			lines.close();
		}
	}

	@Test
	public void testTimeToReadMobyDick() throws FileNotFoundException, IOException {
		long start = System.currentTimeMillis();
		Iterator<String> lines = readFile(m_mobyDick + ".zip");
		while (lines.hasNext()) {
			lines.next();
		}
		System.out.println("Took " + (System.currentTimeMillis() - start) + " millis to read zip " + m_mobyDick);
	}

	@Test
	public void testReadPlainText() throws FileNotFoundException, IOException {
		testLines(readFile(m_textFileName));
	}

	@Test
	public void testReadGzip() throws FileNotFoundException, IOException {
		testLines(readFile(m_gzipName));
	}

	@Test
	public void testReadZip() throws FileNotFoundException, IOException {
		testLines(readFile(m_zipName));
	}
}
