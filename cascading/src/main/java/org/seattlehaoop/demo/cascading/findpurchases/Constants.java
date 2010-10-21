package org.seattlehaoop.demo.cascading.findpurchases;

import java.util.regex.Pattern;

public class Constants {

	
	public static Pattern purchaseLine = Pattern.compile("(\\S+)\\s+INVENTORY\\s+\\(\\d+\\)\\s+\\[(.*?)\\]");
	// "30/Jan/2010:12:00:00    INVENTORY       (33610) [1, 2, 3, 4, 6]";
}
