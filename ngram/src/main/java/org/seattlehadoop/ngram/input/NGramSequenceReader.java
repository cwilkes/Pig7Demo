package org.seattlehadoop.ngram.input;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

public class NGramSequenceReader implements Iterator<TokenAndCounts> {

	private final SequenceFileRecordReader<Text, CountsArray> m_reader;
	private TokenAndCounts m_onDeck;

	public NGramSequenceReader(File sequenceFile) throws IOException, InterruptedException {
		Job job = new Job(new Configuration());
		SequenceFileInputFormat.setInputPaths(job, new Path(sequenceFile.getAbsolutePath()));
		m_reader = new SequenceFileRecordReader<Text, CountsArray>();
		TaskAttemptContext context = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID("ngram", 0, false, 0, 0));
		m_reader.initialize(new FileSplit(new Path(sequenceFile.getAbsolutePath()), 0, sequenceFile.length(), null), context);
		advance();
	}

	private void advance() throws IOException, InterruptedException {
		if (!m_reader.nextKeyValue()) {
			m_reader.close();
			m_onDeck = null;
			return;
		}
		m_onDeck = new TokenAndCounts(m_reader.getCurrentKey().toString(), m_reader.getCurrentValue().getCounts());
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		Iterator<TokenAndCounts> it = new NGramSequenceReader(new File(args[0]));
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}

	@Override
	public boolean hasNext() {
		return m_onDeck != null;
	}

	@Override
	public TokenAndCounts next() {
		TokenAndCounts ret = m_onDeck;
		try {
			advance();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
