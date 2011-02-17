package org.seattlehadoop.ngram;

import static org.seattlehadoop.ngram.Constants.TABLE_NAME;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TokenCountImportTool implements Tool {

	private Configuration m_conf;

	@Override
	public void setConf(Configuration p_conf) {
		m_conf = p_conf;
	}

	@Override
	public Configuration getConf() {
		return m_conf;
	}

	@Override
	public int run(String[] p_args) throws Exception {
		Job job = new Job(getConf());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		int i = 0;
		SequenceFileInputFormat.setInputPaths(job, new Path(p_args[i++]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(p_args[i++]));
		Constants.setQuorum(job.getConfiguration(), "localhost");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(TokenCountImportMapper.class);
		job.setJobName("ngrammer");
		job.setJarByClass(getClass());
		job.setReducerClass(TokenCountImportReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(2);
		job.setCombinerClass(TokenCountImportCombiner.class);
		TableMapReduceUtil.initTableReducerJob(TABLE_NAME, TokenCountImportReducer.class, job);
		Constants.setHostName(job.getConfiguration(), "localhost");
		job.submit();
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TokenCountImportTool(), args);
	}
}
