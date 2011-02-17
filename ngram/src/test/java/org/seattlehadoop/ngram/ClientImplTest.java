package org.seattlehadoop.ngram;

import java.io.IOException;

import org.junit.Test;

public class ClientImplTest {

	@Test
	public void testHitServer() throws IOException {
		Client client = new ClientImpl("aspire.internal.ladro.com");
		System.out.println(client.getTrailingWords("wily"));
	}
}
