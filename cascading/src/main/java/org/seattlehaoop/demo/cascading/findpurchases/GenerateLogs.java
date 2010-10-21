package org.seattlehaoop.demo.cascading.findpurchases;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Random;

public class GenerateLogs {

	private final int[] m_inventory;
	private final Buyer[] m_buyers;
	private final Random m_r;
	public static String DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private static void shuffle(double[] array, Random r) {
		for (int k = array.length; k > 1; k--) {
			double a = array[k - 1];
			int q = r.nextInt(k);
			array[k - 1] = array[q];
			array[q] = a;
		}
	}

	private static void fillRandoms(double[] array, int start, double currentPurchases, Random r) {
		for (int j = 0; j < array.length; j++) {
			array[j] = r.nextDouble() * (1 - currentPurchases);
			currentPurchases += array[j];
		}
	}

	private double[] createPurchaseProfile(int numberItems) {
		double[] purchaseProfile = new double[numberItems];
		fillRandoms(purchaseProfile, 0, 0, m_r);
		shuffle(purchaseProfile, m_r);
		return purchaseProfile;
	}

	private double[] createHourProfile(double singleHourPurchase) {
		double[] hourProfile = new double[24];
		hourProfile[0] = singleHourPurchase;
		fillRandoms(hourProfile, 1, singleHourPurchase, m_r);
		shuffle(hourProfile, m_r);
		return hourProfile;
	}

	public GenerateLogs(int numberBuyers, int numberItems, long seed) {
		m_buyers = new Buyer[numberBuyers];
		m_r = new Random(seed);
		for (int i = 0; i < m_buyers.length; i++) {
			double[] purchaseProfile = createPurchaseProfile(numberItems);
			double[] hourProfile = createHourProfile(0.3);
			// 5 -> 20% of not buying anyhting
			m_buyers[i] = new Buyer(m_r, purchaseProfile, 5 + m_r.nextInt(15), hourProfile, 15 + m_r.nextInt(10));
		}
		m_inventory = new int[numberItems];
		Arrays.fill(m_inventory, 1000);
	}

	public static void main(String[] args) {
		int pos = 0;
		int numBuyers = Integer.parseInt(args[pos++]);
		int numItems = Integer.parseInt(args[pos++]);
		int numberDays = Integer.parseInt(args[pos++]);
		long seed = pos == args.length ? System.currentTimeMillis() : Long.parseLong(args[pos++]);
		GenerateLogs gl = new GenerateLogs(numBuyers, numItems, seed);
		gl.startSimulation(numberDays, System.out);
		gl.printPurchaseHistory(System.err);
		System.out.flush();
		System.err.flush();
	}

	private void printPurchaseHistory(PrintStream out) {
		for (int i = 0; i < m_buyers.length; i++) {
			out.println(String.format("%03d : %s", i, Arrays.toString(m_buyers[i].getPurchaseRecord())));
		}
	}

	private Calendar getStartDate() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 2010);
		cal.set(Calendar.DAY_OF_YEAR, 0);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal;
	}

	private static void shuffle(int[] array, Random r) {
		for (int k = array.length; k > 1; k--) {
			int a = array[k - 1];
			int q = r.nextInt(k);
			array[k - 1] = array[q];
			array[q] = a;
		}
	}

	private void startSimulation(int p_numberDays, PrintStream out) {
		Calendar date = getStartDate();
		int[] buyerOrder = new int[m_buyers.length];
		for (int i = 0; i < buyerOrder.length; i++) {
			buyerOrder[i] = i;
		}
		for (int minute = 0; minute < 24 * 60 * p_numberDays; minute++) {
			date.add(Calendar.MINUTE, 1);
			shuffle(buyerOrder, m_r);
			for (int buyerNumber : buyerOrder) {
				Action buyerStatus = m_buyers[buyerNumber].checkStatus(minute, m_inventory);
				if (buyerStatus != null) {
					out.println(String.format("%s\t%s %d\t%s", sdf.format(date.getTime()), "STATUS", buyerNumber, buyerStatus));
				}
			}
			int inventoryCount = getInventoryCount();
			// 10% chance of writing out inventory
			if (m_r.nextDouble() < 0.10) {
				out.println(String.format("%s\t%s\t(%d)\t%s", sdf.format(date.getTime()), "INVENTORY", inventoryCount, Arrays.toString(m_inventory)));
			}
			if (inventoryCount == 0) {
				break;
			}
		}
		int inventoryCount = getInventoryCount();
		out.println(String.format("%s\t%s\t(%d)\t%s", sdf.format(date.getTime()), "INVENTORY", inventoryCount, Arrays.toString(m_inventory)));
	}

	private int getInventoryCount() {
		int count = 0;
		for (int inventoryLeft : m_inventory) {
			count += inventoryLeft;
		}
		return count;
	}
}
