package org.seattlehadoop.fingerprint.reader;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

public class LineReader implements Iterator<String>, Closeable {

	private final BufferedReader m_br;
	private String m_line;

	public static enum FileType {
		GZIP, ZIP;
	}

	public static FileType getFileType(String fileName) {
		String[] parts = fileName.split("\\.");
		if (parts.length == 1) {
			return null;
		}
		String ending = parts[parts.length - 1].toLowerCase();
		if (ending.equals("zip")) {
			return FileType.ZIP;
		}
		if (ending.equals("gz")) {
			return FileType.GZIP;
		}
		return null;
	}

	public static LineReader readFile(File file) throws FileNotFoundException, IOException {
		FileType fileType = getFileType(file.getName());
		if (fileType == null) {
			return new LineReader(new FileReader(file));
		}
		if (fileType == FileType.GZIP) {
			return new LineReader(new GZIPInputStream(new FileInputStream(file)));
		}
		ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
		zis.getNextEntry();
		return new LineReader(zis);
	}

	public LineReader(Reader reader) {
		m_br = new BufferedReader(reader);
		advance();
	}

	public LineReader(InputStream is) {
		this(new InputStreamReader(is));
	}

	private void advance() {
		try {
			m_line = m_br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			m_line = null;
		}
		if (m_line == null) {
			try {
				close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean hasNext() {
		return m_line != null;
	}

	@Override
	public String next() {
		String ret = m_line;
		advance();
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		m_br.close();
	}

}
