package org.seattlehadoop.demo.pig.loadfunc;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class ExifFilter extends FilterFunc {

	private final String term = "blackhole";
	private final Pattern m_termPattern = Pattern.compile("\b" + term + "\b");

	protected String getCommentLine(String fileName, byte[] bytes) throws IOException {
		File tmpJpg = null;
		Process process = null;
		try {
			tmpJpg = File.createTempFile(getClass().getName() + "-", ".tmp");
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(tmpJpg));
			bos.write(bytes);
			bos.close();
			Runtime runtime = Runtime.getRuntime();
			process = runtime.exec("/usr/bin/exiftool -list " + tmpJpg.getAbsolutePath());
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String s;
			while ((s = br.readLine()) != null) {
				if (s.startsWith("Comment")) {
					return s.split(":")[1];
				}
			}
			br.close();
			process.destroy();
		} finally {
			tmpJpg.delete();
			process.destroy();
		}
		System.out.println("BAD: " + fileName);
		return null;
	}

	@Override
	public Boolean exec(Tuple p_t) throws IOException {
		Object term = p_t.get(0);
		String comment = getCommentLine((String) p_t.get(1), (byte[]) p_t.get(2));
		if (comment != null && m_termPattern.matcher(comment).matches()) {
			return true;
		}
		return false;
	}

}
