package org.seattlehadoop.mahout;

import static org.junit.Assert.*;

import org.junit.Test;

public class ClusterKeyTest {

	@Test
	public void testLeadingZeros() {
		ClusterKey key = ClusterKey.makeFromString("hi-007");
		assertEquals("hi", key.getClusterName());
		assertEquals(7, key.getItemNumber());
	}
	
	@Test
	public void testDashesInName() {
		ClusterKey key = ClusterKey.makeFromString("hi-there-123");
		assertEquals("hi-there", key.getClusterName());
		assertEquals(123, key.getItemNumber());
	}
}
