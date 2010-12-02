package org.seattlehadoop.mahout;

import uk.co.flamingpenguin.jewel.cli.Option;

public interface PlanetCreationOptions {

	// @Option(shortName = "d", defaultValue="1000")
	// int getDimension();

	// @Option(shortName = "e", defaultValue="5")
	// int getElements();

	// @Option(shortName = "j")
	// double getJitter();

	@Option(shortName = "b", defaultValue = "100")
	int getBodies();

	@Option(shortName = "c", defaultValue = "5")
	int getClusters();

	@Option(shortName = "o", defaultValue = "output")
	String getOutputDirectory();

	@Option(shortName = "p", defaultValue = "my")
	String getClusterPrefix();

	@Option(shortName = "r", defaultValue = "50")
	int getRadius();

	@Option(shortName = "s", defaultValue = "1000")
	int getGridSize();

	@Option(shortName = "t", description = "the cluster class to use, ie sh.PlanetCluster1")
	String getClusterClass();

}
