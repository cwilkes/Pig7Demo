package org.seattlehadoop.ngram.mapreduce;

import static org.seattlehadoop.ngram.mapreduce.Constants.TABLE_NAME;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TokenCountShowTool implements Tool {

	private Configuration m_conf;

	@Override
	public void setConf(Configuration p_conf) {
		m_conf = p_conf;
	}

	@Override
	public Configuration getConf() {
		return m_conf;
	}

	protected Job makeJob(Path input, Path output) throws IOException {
		Job job = new Job(getConf());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, input);
		SequenceFileOutputFormat.setOutputPath(job, output);
		Constants.setQuorum(job.getConfiguration(), "localhost");
		job.setJobName("ngrammer");
		job.setJarByClass(getClass());
		job.setNumReduceTasks(2);
		Scan scan = new Scan();
		// scan.addColumn(Constants.COLUMN_FAMILY);
		// scan.setFilter(new FirstKeyOnlyFilter());
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, TokenCountShowMapper.class, Text.class, IntWritable.class, job);
		Constants.setHostName(job.getConfiguration(), "u1.internal");
		return job;
	}

	@Override
	public int run(String[] p_args) throws Exception {
		int i = 0;
		Job job = makeJob(new Path(p_args[i++]), new Path(p_args[i++]));
		job.submit();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TokenCountImportTool(), args);
	}
}
