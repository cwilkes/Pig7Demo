package org.seattlehadoop.ngram.input;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

}
