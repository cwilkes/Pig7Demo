package org.seattlehadoop.ngram.input;

import java.io.IOException;
import java.io.Reader;

public interface ReaderSupplier<T> {

	Reader openReader(T inputType) throws IOException;
}
