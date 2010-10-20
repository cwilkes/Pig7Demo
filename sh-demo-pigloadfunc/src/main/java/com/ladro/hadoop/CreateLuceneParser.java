package com.ladro.hadoop;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.cmates.wopr.utils.InputArg;

public class CreateLuceneParser {
	@InputArg("input")
	private List<Path> m_inputPaths;
	@InputArg("output")
	private Path m_outputPath;

	@InputArg("reducers")
	private int m_reducers = -1;

	public int getReducers() {
		return m_reducers;
	}

	public boolean isReducersSet() {
		return m_reducers != -1;
	}

	public void setReducers(int p_reducers) {
		m_reducers = p_reducers;
	}

	public void setOutputPath(Path p_outputPath) {
		m_outputPath = p_outputPath;
	}

	public Path getOutputPath() {
		return m_outputPath;
	}

	public List<Path> getInputPaths() {
		return m_inputPaths;
	}

	public void setInputPaths(List<Path> p_inputPaths) {
		m_inputPaths = p_inputPaths;
	}

}
