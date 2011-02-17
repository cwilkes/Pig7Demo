package org.seattlehadoop.ngram;

import java.io.Closeable;
import java.io.IOException;

public interface Client extends Closeable {

	TrailingWords getTrailingWords(String firstWord) throws IOException;
}
