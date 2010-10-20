package org.seattlehadoop.demo.pig.loadfunc;

import static org.seattlehadoop.demo.pig.loadfunc.FileUtil.publishFiles;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceTarFileGenerator implements Closeable {

	private final Writer m_writer;
	private int m_count;

	public SequenceTarFileGenerator(File outputFile) throws IOException {
		Configuration conf = new Configuration();
		Path outputPath = new Path(outputFile.getAbsolutePath());
		m_writer = SequenceFile.createWriter(outputPath.getFileSystem(conf), conf, outputPath, Text.class, BytesWritable.class,
				SequenceFile.CompressionType.BLOCK);
	}

	public void addFile(String name, File p_file) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(p_file));
		byte[] buf = new byte[(int) p_file.length()];
		bis.read(buf);
		bis.close();
		m_writer.append(new Text(name), new BytesWritable(buf));
		m_count++;
	}

	public void addAllFilesBeneath(final File topdir) {
		int initialCount = 0;
		publishFiles(topdir, new FilePublisher() {

			@Override
			public void publish(File p_file) {
				try {
					addFile(p_file.getAbsolutePath().substring(topdir.getAbsolutePath().length()), p_file);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("Added " + (m_count - initialCount) + " files under " + topdir);
	}

	public static void main(String[] args) throws IOException {
		if (args.length <= 1) {
			System.err.println("Pass me an input and output");
		}
		final SequenceTarFileGenerator gen = new SequenceTarFileGenerator(new File(args[0]));
		for (int i = 1; i < args.length; i++) {
			gen.addAllFilesBeneath(new File(args[i]));
		}
		gen.close();
		System.out.println("Done");
	}

	@Override
	public void close() throws IOException {
		m_writer.close();
	}
}
