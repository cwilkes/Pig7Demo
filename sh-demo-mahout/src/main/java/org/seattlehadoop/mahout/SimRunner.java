package org.seattlehadoop.mahout;

import static org.seattlehadoop.mahout.Utils.NEWLINE;
import static org.seattlehadoop.mahout.Utils.TAB;
import static uk.co.flamingpenguin.jewel.cli.CliFactory.parseArguments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;

public class SimRunner {
	private static final String OUTFILE_META = "%s-planetsMeta.txt";
	private static final String OUTFILE_DATA = "%s-planetsData.tsv";

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		try {
			PlanetCreationOptions options = parseArguments(PlanetCreationOptions.class, args);
			File[] outputFiles = runSimulation(options);
			System.out.println("Data file: " + outputFiles[1]);
		} catch (ArgumentValidationException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	public static <T> File[] runSimulation(final PlanetCreationOptions options) throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IOException, InterruptedException {
		File metaFile = new File(options.getOutputDirectory(), String.format(OUTFILE_META, options.getClusterPrefix()));
		File dataFile = new File(options.getOutputDirectory(), String.format(OUTFILE_DATA, options.getClusterPrefix()));
		if (!metaFile.getParentFile().isDirectory()) {
			metaFile.getParentFile().mkdirs();
		}
		ClusterService<T> clusterConverter = Utils.createClusterConverter(options.getClusterClass());
		final BufferedWriter output = new BufferedWriter(new FileWriter(dataFile));
		String outputFormat = "%s" + TAB + "%s" + NEWLINE;
		output.append("CLUSTER" + TAB + "PLANET" + TAB + clusterConverter.getHeader() + NEWLINE);
		Random r = new Random();
		for (int clusterNumber = 1; clusterNumber <= options.getClusters(); clusterNumber++) {
			Iterator<T> planets = clusterConverter.createPlanetInCluster(options, r);
			int itemNumber = 0;
			while (planets.hasNext()) {
				ClusterKey key = new ClusterKey(Integer.toString(clusterNumber), itemNumber++);
				T data = planets.next();
				output.write(String.format(outputFormat, key, data));
			}
		}
		output.close();
		return new File[] { metaFile, dataFile };

	}
}
