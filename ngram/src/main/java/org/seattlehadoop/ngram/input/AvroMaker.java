package org.seattlehadoop.ngram.input;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.seattlehadoop.ngram.avro.TokenCount;

import com.google.common.base.Function;

public class AvroMaker implements Closeable {

	private final List<File> m_input;
	private final long m_maxFileSize;
	private final File m_outputDir;
	private final String m_filePrefix;

	private DataFileWriter<TokenCount> m_writer;
	private File m_currentFile;
	private int m_count;

	public AvroMaker(List<File> p_input, File p_outputDir, String filePrefix, long p_maxFileSize) throws IOException {
		m_input = p_input;
		m_outputDir = p_outputDir;
		m_filePrefix = filePrefix;
		m_maxFileSize = p_maxFileSize;
	}

	private void initWriter() throws IOException {
		m_writer = new DataFileWriter<TokenCount>(new SpecificDatumWriter<TokenCount>(TokenCount.class));
		m_writer.setCodec(CodecFactory.deflateCodec(1));
		m_currentFile = File.createTempFile(m_filePrefix, ".avro", m_outputDir);
		m_writer.create(TokenCount.SCHEMA$, m_currentFile);
	}

	public CloseableIterator<TokenCount> readInputs() throws IOException {
		return readFiles(m_input.iterator());
	}

	public static CloseableIterator<TokenCount> readFiles(final Iterator<File> inputs) throws IOException {
		final Function<String, TokenCount> converter = new TokenCountFromLineFunction();
		return new CloseableIterator<TokenCount>() {
			private FilteredLineProcessor<TokenCount> m_onDeckIterator;
			private TokenCount m_onDeckRet;
			private boolean m_done;
			{
				advance();
			}

			@Override
			public boolean hasNext() {
				return !m_done;
			}

			private void advance() {
				if (m_onDeckIterator == null) {
					if (!inputs.hasNext()) {
						m_done = true;
						try {
							close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						return;
					}
					File f = inputs.next();
					try {
						m_onDeckIterator = new FilteredLineProcessor<TokenCount>(new FileInputSupplier(f).getInput(), converter);
					} catch (IOException ex) {
						throw new IllegalStateException("Error reading in " + f.getAbsolutePath(), ex);
					}
				}
				if (!m_onDeckIterator.hasNext()) {
					try {
						m_onDeckIterator.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					m_onDeckIterator = null;
					advance();
					return;
				}
			}

			@Override
			public TokenCount next() {
				TokenCount tc = m_onDeckRet;
				advance();
				return tc;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void close() throws IOException {
				if (m_onDeckIterator != null) {
					m_onDeckIterator.close();
				}
			}
		};

	}

	public void add(TokenCount tc) throws IOException {
		if (m_writer == null) {
			initWriter();
		}
		m_count++;
		m_writer.append(tc);
		if (m_currentFile.length() > m_maxFileSize) {
			System.out.println("Wrote out " + m_count + " entries");
			m_count = 0;
			m_writer.close();
			m_writer = null;
		}
	}

	public static void main(String[] args) throws IOException {
		AvroMakerArgParser parser = new AvroMakerArgParser(args);
		AvroMaker avroMaker = new AvroMaker(parser.getInput(), parser.getOutput(), "tokencount-", parser.getMaxFileSize());
		for (Iterator<TokenCount> it = avroMaker.readInputs(); it.hasNext();) {
			avroMaker.add(it.next());
		}
		avroMaker.close();
	}

	@Override
	public void close() throws IOException {
		if (m_writer != null) {
			System.out.println("Wrote out " + m_count + " entries");
			m_count = 0;
			m_writer.close();
		}
		m_currentFile = null;
	}

}
