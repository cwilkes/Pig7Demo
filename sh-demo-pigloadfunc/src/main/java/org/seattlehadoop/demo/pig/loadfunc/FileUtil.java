package org.seattlehadoop.demo.pig.loadfunc;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

	public static List<File> getAllFiles(File topDir) {
		List<File> ret = new ArrayList<File>();
		recurse(topDir, ret, new FileFilter() {

			@Override
			public boolean accept(File p_pathname) {
				return p_pathname.isFile();
			}
		}, new FileFilter() {

			@Override
			public boolean accept(File p_pathname) {
				return true;
			}
		});
		return ret;
	}

	public static void publishFiles(File myDirOrFile, FilePublisher publisher) {
		if (myDirOrFile.isFile()) {
			publisher.publish(myDirOrFile);
			return;
		}
		File[] dirs = myDirOrFile.listFiles();
		if (dirs != null) {
			for (File d : dirs) {
				publishFiles(d, publisher);
			}
		}
	}

	public static void recurse(File myDirOrFile, List<File> allFiles, FileFilter outputFile, FileFilter inputFilter) {
		if (outputFile.accept(myDirOrFile)) {
			allFiles.add(myDirOrFile);
		}
		File[] dirs = myDirOrFile.listFiles(inputFilter);
		if (dirs != null) {
			for (File d : dirs) {
				recurse(d, allFiles, outputFile, inputFilter);
			}
		}
	}
}
