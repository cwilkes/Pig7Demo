package com.ladro.hadoop;

import static com.cmates.wopr.utils.CLIInput.createParser;

import java.util.Arrays;
import a.b.MyKey;
import c.d.MyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateLuceneTool implements Tool {
	private static final int DEFAULT_NUMBER_REDUCERS = 4;
	private Configuration m_conf;

	@Override
	public void setConf(Configuration conf) {
		m_conf = conf;
	};

	@Override
	public Configuration getConf() {
		return m_conf;
	};

	public static void main(String[] args) throws Exception {
		System.out.println("Running with " + Arrays.asList(args) + "");
		ToolRunner.run(new CreateLuceneTool(), args);
	}

	@Override
	public int run(String[] p_args) throws Exception {
		CreateLuceneParser parser = createParser(CreateLuceneParser.class, p_args);
		Job job = new Job(getConf());
		FileInputFormat.setInputPaths(job, parser.getInputPaths().toArray(new Path[0]));
		FileOutputFormat.setOutputPath(job, parser.getOutputPath());
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(MyValue.class);
		job.setMapperClass(CreateLuceneMapper.class);
		job.setReducerClass(CreateLuceneReducer.class);
		job.setNumReduceTasks(parser.isReducersSet() ? parser.getReducers() : DEFAULT_NUMBER_REDUCERS);
		job.submit();
		System.out.println(job.getTrackingURL());
		return 0;
	}

}
