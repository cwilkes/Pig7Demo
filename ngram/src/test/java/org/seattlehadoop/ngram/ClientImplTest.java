package org.seattlehadoop.ngram;

import java.io.IOException;

import org.junit.Test;

public class ClientImplTest {

	@Test
	public void testHitServer() throws IOException {
		Client client = new ClientImpl("localhost");
		System.out.println(client.getTrailingWords("wily"));
	}
}
