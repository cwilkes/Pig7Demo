package org.seattlehadoop.mahout.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.Pair;
import static org.seattlehadoop.mahout.Utils.*;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

public class BoxPlotWriter {

	public static Iterator<BoxedCluster> readSeqFile(File seqFile) throws IOException, InterruptedException {
		SequenceFileReader<Text, Cluster> it = new SequenceFileReader<Text, Cluster>(seqFile, Text.class, Cluster.class);
		Function<Pair<Text, Cluster>, BoxedCluster> clusterTransformer = new ClusterTransformer();
		return Iterators.transform(it, clusterTransformer);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		int pos = 0;
		Iterator<BoxedCluster> boxedClusters = readSeqFile(new File(args[pos++]));
		String[] header = new String[] { "CLUSTER", "X", "Y", "WIDTH", "HEIGHT", "BRIGHT" };
		Function<BoxedCluster, String> boxedClusterToTabbed = new BoxedClusterToTabbed();
		System.out.println(Joiner.on(TAB).join(header));
		while (boxedClusters.hasNext()) {
			BoxedCluster bc = boxedClusters.next();
			System.out.println(boxedClusterToTabbed.apply(bc));
		}
	}
}
