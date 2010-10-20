package org.seattlehaoop.demo.cascading.findpurchases;

import java.util.Random;

public class Buyer {

	private final double[] m_purchaseProfile;
	private final Random m_r;
	private final double[] m_hourProfile;
	private final int m_visitTime;
	private Status m_currentStatus;
	private int m_minuteWhenChangedStatus = -100000;

	private static enum Status {
		ONLINE, OFFLINE;
	}

	public Buyer(Random r, double[] purchaseProfile, double[] hourProfile, int visitTime) {
		m_r = r;
		m_purchaseProfile = purchaseProfile;
		m_hourProfile = hourProfile;
		m_visitTime = visitTime;
		for (int i = 1; i < m_purchaseProfile.length; i++) {
			m_purchaseProfile[i] += m_purchaseProfile[i - 1];
		}
		m_currentStatus = Status.OFFLINE;
	}

	public Action checkStatus(int minuteOfDay, int[] inventory) {
		if (m_currentStatus == Status.OFFLINE) {
			int hour = (minuteOfDay / 60) % 24;
			if (m_r.nextDouble() > m_hourProfile[hour]) {
				// staying at home
				return null;
			}
			if (m_minuteWhenChangedStatus + 60 >= minuteOfDay) {
				// don't go back within an hour of leaving
				return null;
			}
			m_minuteWhenChangedStatus = minuteOfDay;
			m_currentStatus = Status.ONLINE;
			return Action.ENTER;
		} else {
			int timeOnSite = minuteOfDay - m_minuteWhenChangedStatus;
			if (timeOnSite > m_visitTime && m_r.nextDouble() < 0.33) {
				m_currentStatus = Status.OFFLINE;
				m_minuteWhenChangedStatus = minuteOfDay;
				return Action.LEAVE;
			}
		}
		// online and haven't left. Always will return null to indicate that no
		// change in status
		for (int i = 0; i < 5; i++) {
			double selection = m_r.nextDouble();
			if (selection > m_purchaseProfile[m_purchaseProfile.length - 1]) {
				// don't buy anything
				return null;
			}
			for (int itemNumber = inventory.length - 2; itemNumber >= 0; itemNumber--) {
				if (selection > m_purchaseProfile[itemNumber] && inventory[itemNumber + 1] > 0) {
					inventory[itemNumber + 1]--;
					return null;
				}
			}
			if (inventory[0] > 0) {
				inventory[0]--;
				return null;
			}
		}
		// tried 5 times to buy an item but none were left, give up
		return null;
	}
}
