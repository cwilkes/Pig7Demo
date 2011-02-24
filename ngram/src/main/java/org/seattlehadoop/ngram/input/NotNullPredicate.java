package org.seattlehadoop.ngram.input;

import com.google.common.base.Predicate;

public class NotNullPredicate<T> implements Predicate<T> {

	@Override
	public boolean apply(T p_input) {
		return p_input != null;
	}

}
