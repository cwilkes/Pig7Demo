package org.seattlehadoop.fingerprint;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.seattlehadoop.fingerprint.reader.LowerCaseFunction;
import org.seattlehadoop.fingerprint.reader.ShingleUtil;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;

public class CLI {

	public static Multiset<String> countShingles(File file) throws FileNotFoundException, IOException {
		final Multiset<String> shingleCounts = HashMultiset.create();
		for (Iterator<String> shingles = Iterators.transform(ShingleUtil.makeParagraphs(file), new LowerCaseFunction()); shingles.hasNext();) {
			shingleCounts.add(shingles.next());
		}
		return shingleCounts;
	}

	public static Map<String, Double> countShinglesAndSort(File file) throws FileNotFoundException, IOException {
		final Multiset<String> shingleCounts = countShingles(file);
		int maxSize = -1;
		for (Entry<String> e : shingleCounts.entrySet()) {
			if (e.getCount() > maxSize) {
				maxSize = e.getCount();
			}
		}
		Map<String, Double> shingleCount2 = new TreeMap<String, Double>(new Comparator<String>() {

			@Override
			public int compare(String p_o1, String p_o2) {
				return shingleCounts.count(p_o2) - shingleCounts.count(p_o1);
			}
		});
		for (Entry<String> e : shingleCounts.entrySet()) {
			shingleCount2.put(e.getElement(), 1.0 * e.getCount() / maxSize);
		}
		return shingleCount2;
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Map<String, Map<String, Double>> countsByBook = new HashMap<String, Map<String, Double>>();
		for (String bookName : args) {
			File f = new File(bookName);
			countsByBook.put(f.getName(), countShinglesAndSort(f));
		}
		Iterator<String> bookNames = countsByBook.keySet().iterator();
		String book1 = bookNames.next();
		String book2 = bookNames.next();
		final Map<String, Double> diff = new HashMap<String, Double>();
		for (Map.Entry<String, Double> word : countsByBook.get(book1).entrySet()) {
			Double otherScore = countsByBook.get(book2).get(word.getKey());
			diff.put(word.getKey(), word.getValue() - (otherScore == null ? 0 : otherScore));
		}
		for (Map.Entry<String, Double> word : countsByBook.get(book2).entrySet()) {
			if (!countsByBook.get(book1).containsKey(word.getKey())) {
				Double otherScore = null; // countsByBook.get(book2).get(word.getKey());
				diff.put(word.getKey(), (otherScore == null ? 0 : otherScore) - word.getValue());
			}
		}
		List<String> words = new ArrayList<String>(diff.keySet());
		Collections.sort(words, new Comparator<String>() {

			@Override
			public int compare(String p_o1, String p_o2) {
				double d = diff.get(p_o2) - diff.get(p_o1);
				return (d < 0) ? -1 : (d > 0) ? 1 : 0;
			}
		});
		for (String w : words) {
			System.out.println(String.format("%s %10.9f", w, diff.get(w)));
		}
	}
}
