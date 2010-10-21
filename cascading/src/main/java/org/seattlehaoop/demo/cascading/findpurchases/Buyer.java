package org.seattlehaoop.demo.cascading.findpurchases;

import java.util.Random;

public class Buyer {

	private final double[] m_purchaseProfile;
	private final Random m_r;
	private final double[] m_hourProfile;
	private final int m_visitTime;
	private Status m_currentStatus;
	private int m_minuteWhenChangedStatus = -100000;
	private int m_goingToPurchaseTime, m_goingToLeaveTime;
	private final double m_chancePurchase;

	private final int[] m_purchaseRecord;

	private static enum Status {
		ONLINE, OFFLINE;
	}

	public Buyer(Random r, double[] purchaseProfile, double chancePurchase, double[] hourProfile, int visitTime) {
		m_r = r;
		m_purchaseRecord = new int[purchaseProfile.length];
		m_purchaseProfile = purchaseProfile;
		m_chancePurchase = chancePurchase;
		m_hourProfile = hourProfile;
		m_visitTime = visitTime;
		for (int i = 1; i < m_purchaseProfile.length; i++) {
			m_purchaseProfile[i] += m_purchaseProfile[i - 1];
		}
		m_currentStatus = Status.OFFLINE;
	}

	private void doPurchase(int[] inventory, int itemNumber) {
		inventory[itemNumber]--;
		m_purchaseRecord[itemNumber]++;
	}

	public int[] getPurchaseRecord() {
		return m_purchaseRecord;
	}

	private void attemptPurchase(int[] inventory) {
		for (int i = 0; i < 5; i++) {
			double selection = m_r.nextDouble();
			if (selection > m_purchaseProfile[m_purchaseProfile.length - 1]) {
				// don't buy anything
				return;
			}
			for (int itemNumber = inventory.length - 2; itemNumber >= 0; itemNumber--) {
				if (selection > m_purchaseProfile[itemNumber] && inventory[itemNumber + 1] > 0) {
					doPurchase(inventory, itemNumber + 1);
					return;
				}
			}
			if (inventory[0] > 0) {
				doPurchase(inventory, 0);
				return;
			}
		}
		// didn't buy anything
	}

	private Action doOfflineActions(int minuteOfDay) {
		int hour = (minuteOfDay / 60) % 24;
		if (m_r.nextDouble() > m_hourProfile[hour]) {
			// staying at home
			return null;
		}
		if (m_minuteWhenChangedStatus + 60 >= minuteOfDay) {
			// don't go back within an hour of leaving
			return null;
		}
		if (m_r.nextDouble() < m_chancePurchase) {
			m_goingToPurchaseTime = minuteOfDay + m_r.nextInt(m_visitTime);
			m_goingToLeaveTime = m_goingToPurchaseTime + m_r.nextInt(1 + m_goingToPurchaseTime - minuteOfDay + m_visitTime);
		} else {
			m_goingToPurchaseTime = Integer.MAX_VALUE;
			m_goingToLeaveTime = minuteOfDay + m_r.nextInt(m_visitTime);
		}
		m_minuteWhenChangedStatus = minuteOfDay;
		m_currentStatus = Status.ONLINE;
		return Action.ENTER;
	}

	public Action checkStatus(int minuteOfDay, int[] inventory) {
		if (m_currentStatus == Status.OFFLINE) {
			return doOfflineActions(minuteOfDay);
		}
		if (minuteOfDay >= m_goingToPurchaseTime) {
			attemptPurchase(inventory);
			m_goingToPurchaseTime = Integer.MAX_VALUE;
		}
		if (minuteOfDay >= m_goingToLeaveTime) {
			m_currentStatus = Status.OFFLINE;
			m_minuteWhenChangedStatus = minuteOfDay;
			return Action.LEAVE;
		}
		return null;
	}
}
