package org.seattlehaoop.demo.cascading.findpurchases;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;

import org.junit.Test;

public class ConstantsTest {

	@Test
	public void testInventoryLine() {
		String line = "30/Jan/2010:12:00:00    INVENTORY       (33610) [1, 2, 3, 4, 6]";
		Matcher m = Constants.purchaseLine.matcher(line);
		assertTrue(m.find());
		assertEquals("30/Jan/2010:12:00:00", m.group(1));
		assertEquals("1, 2, 3, 4, 6", m.group(2));
		assertEquals(2, m.groupCount());
	}

	@Test
	public void testEnterLeaveLine() {
		String leave = "01/Jan/2010:18:03:00    STATUS 42       LEAVE";
		String enter = "01/Jan/2010:18:03:00    STATUS 4        ENTER";
		Matcher m = null; // Constants.movementLine.matcher(leave);
		assertTrue(m.matches());
		assertEquals("01/Jan/2010:18:03:00", m.group(1));
		assertEquals("42", m.group(2));
		assertEquals("LEAVE", m.group(3));
		
		//m = Constants.movementLine.matcher(enter);
		assertTrue(m.matches());
		assertEquals("01/Jan/2010:18:03:00", m.group(1));
		assertEquals("4", m.group(2));
		assertEquals("ENTER", m.group(3));
	}
}
