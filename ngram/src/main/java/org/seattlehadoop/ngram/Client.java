package org.seattlehadoop.ngram;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Client extends Closeable {

	TrailingWords getTrailingWords(String firstWord) throws IOException;

	Map<Integer, Integer> getCountsByYear(String firstWord, String secondWord) throws IOException;

	List<PairedWordScores> parseDocment(List<String> lines) throws IOException;
}
