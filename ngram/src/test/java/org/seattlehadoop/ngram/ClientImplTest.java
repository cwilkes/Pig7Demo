package org.seattlehadoop.ngram;

import java.io.IOException;

import org.junit.Test;
import org.seattlehadoop.ngram.mapreduce.Client;
import org.seattlehadoop.ngram.mapreduce.ClientImpl;

public class ClientImplTest {

	@Test
	public void testHitServer() throws IOException {
		Client client = new ClientImpl("u1.internal");
		System.out.println(client.getTrailingWords("wily"));
	}
}
