package org.seattlehadoop.mahout;

import static org.junit.Assert.*;

import org.junit.Test;

public class ClusterKeyTest {

	@Test
	public void testLeadingZeros() {
		ClusterKey key = ClusterKey.makeFromString(new ClusterKey("leading", 007).toString());
		assertEquals("leading", key.getClusterName());
		assertEquals(7, key.getItemNumber());
	}

	@Test
	public void testDashesInName() {
		ClusterKey key = ClusterKey.makeFromString(new ClusterKey("hi-there", 123).toString());
		assertEquals("hi-there", key.getClusterName());
		assertEquals(123, key.getItemNumber());
	}
}
