package org.seattlehadoop.fingerprint.reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import static org.seattlehadoop.fingerprint.reader.LineReader.*;

public class ShingleUtil {

	public static Iterator<String> makeShingles(File inputFile) throws FileNotFoundException, IOException {
		return makeShingles(inputFile, 3);
	}

	public static Iterator<String> makeParagraphs(File inputFile) throws FileNotFoundException, IOException {
		return new ParagraphMaker(readFile(inputFile));
	}

	public static Iterator<String> makeShingles(File inputFile, int lettersInShingle) throws FileNotFoundException, IOException {
		return new ShingleMaker(makeParagraphs(inputFile), lettersInShingle);
	}
}
