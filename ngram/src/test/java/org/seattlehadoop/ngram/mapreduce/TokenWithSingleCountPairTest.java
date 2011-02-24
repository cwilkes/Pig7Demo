package org.seattlehadoop.ngram.mapreduce;

import static org.junit.Assert.*;

import org.junit.Test;
import org.seattlehadoop.ngram.mapreduce.groupedtoken.TokenWithSingleCountPair;


public class TokenWithSingleCountPairTest {

	@Test
	public void testSerialize() {
		TokenWithSingleCountPair token = new TokenWithSingleCountPair("aaa bbb", 1921, 134);		
		assertEquals(134, token.value().get(1921).intValue());
	}
}
