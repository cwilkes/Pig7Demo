package org.seattlehadoop.ngram.input;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.io.InputSupplier;

public class FileInputSupplier implements InputSupplier<InputStreamReader> {

	private final File m_file;

	public FileInputSupplier(File file) {
		m_file = file;
	}

	private boolean isZip() {
		return m_file.getName().endsWith(".zip");
	}

	private boolean isGZip() {
		return m_file.getName().endsWith(".gz");
	}

	@Override
	public InputStreamReader getInput() throws IOException {
		if (isZip()) {
			ZipInputStream zis = new ZipInputStream(new FileInputStream(m_file));
			ZipEntry ze = zis.getNextEntry();
			// use only the first file in a zip, change to all files later
			while (ze.isDirectory()) {
				ze = zis.getNextEntry();
				if (ze == null) {
					break;
				}
			}
			if (ze == null) {
				throw new IllegalStateException("No file in zip " + m_file);
			}
			return new InputStreamReader(zis);
		}
		if (isGZip()) {
			return new InputStreamReader(new GZIPInputStream(new FileInputStream(m_file)));
		}
		return new InputStreamReader(new FileInputStream(m_file));
	}

}
