package org.seattlehadoop.ngram.mapreduce.groupedtoken;

import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.seattlehadoop.ngram.avro.GroupedToken;
import org.seattlehadoop.ngram.avro.TokenCount;

public class GroupedTokenTool implements Tool {

	private Configuration m_conf;

	public JobConf makeJob() {
		JobConf job = new JobConf(getConf());
		AvroJob.setInputSchema(job, TokenCount.SCHEMA$);
		AvroJob.setOutputSchema(job, GroupedToken.SCHEMA$);
		AvroJob.setMapOutputSchema(job, TokenWithSingleCountPair.getMySchema());
		AvroJob.setMapperClass(job, TokenCountByYearMapper.class);
		AvroJob.setReducerClass(job, TokenCountByYearReducer.class);
		AvroJob.setCombinerClass(job, TokenCountByYearCombiner.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GroupedTokenTool(), args);
	}

	@Override
	public void setConf(Configuration p_conf) {
		m_conf = p_conf;
	}

	@Override
	public Configuration getConf() {
		return m_conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = makeJob();
		jobConf.setJarByClass(getClass());
		int pos = 0;
		FileInputFormat.setInputPaths(jobConf, new Path(args[pos++]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[pos++]));
		JobClient client = new JobClient(jobConf);
		client.submitJob(jobConf);
		return 0;
	}
}
