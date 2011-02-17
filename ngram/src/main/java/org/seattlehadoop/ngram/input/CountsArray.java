package org.seattlehadoop.ngram.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class CountsArray extends ArrayWritable {

	public CountsArray() {
		super(IntWritable.class);
	}

	public CountsArray(List<int[]> p_counts) {
		this();
		int numberPerRow = p_counts.isEmpty() ? 0 : p_counts.get(0).length;
		IntWritable[] toSet = new IntWritable[numberPerRow * p_counts.size() + 1];
		int i = 0;
		toSet[i++] = new IntWritable(numberPerRow);
		for (int[] count : p_counts) {
			for (int c : count) {
				toSet[i++] = new IntWritable(c);
			}
		}
		set(toSet);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int[] count : getCounts()) {
			if (sb.length() > 0) {
				sb.append(";");
			}
			sb.append(Arrays.toString(count));
		}
		return sb.toString();
	}

	public List<int[]> getCounts() {
		int pos = 0;
		int numberPerRow = ((IntWritable) get()[pos++]).get();
		List<int[]> ret = new ArrayList<int[]>();
		while (pos < get().length) {
			int[] entry = new int[numberPerRow];
			for (int i = 0; i < numberPerRow; i++) {
				entry[i] = ((IntWritable) get()[pos++]).get();
			}
			ret.add(entry);
		}
		return ret;
	}
}
