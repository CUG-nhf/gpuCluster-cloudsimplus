/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.network;

import org.cloudbus.cloudsim.network.topologies.TopologicalGraph;
import org.cloudbus.cloudsim.network.topologies.TopologicalLink;
import org.cloudbus.cloudsim.util.Util;

/**
 * Represents matrix containing the delay (in seconds) between every pair or nodes
 * inside a network topology. It stores every distance between connected nodes.
 *
 * @author Thomas Hohnstein
 * @since CloudSim Toolkit 1.0
 */
public class DelayMatrix {

    /**
	 * Matrix holding delay between any pair of nodes (in seconds).
	 */
    private double[][] mDelayMatrix;

	/**
	 * Number of nodes in the distance-aware-topology.
	 */
    private int mTotalNodeNum;

    /**
     * Creates an empty matrix with no columns or rows.
     */
	public DelayMatrix() {
        mDelayMatrix = new double[0][0];
	}

	/**
	 * Creates a Delay Matrix for a given network topology graph.
	 *
	 * @param graph the network topological graph
	 * @param directed indicates if a directed matrix should be computed (true) or not (false)
	 */
	public DelayMatrix(final TopologicalGraph graph, final boolean directed) {
		createDelayMatrix(graph, directed);
		calculateShortestPath();
	}

	/**
     * Gets the delay between two nodes.
     *
	 * @param srcID the id of the source node
	 * @param destID the id of the destination node
	 * @return the delay between the given two nodes
	 */
	public double getDelay(final int srcID, final int destID) {
		if (srcID > mTotalNodeNum || destID > mTotalNodeNum) {
			throw new ArrayIndexOutOfBoundsException("srcID or destID is higher than highest stored node-ID!");
		}

		return mDelayMatrix[srcID][destID];
	}

	/**
	 * Creates all internal necessary network-distance structures from the given graph.
         * For similarity, we assume all communication-distances are symmetrical,
         * thus leading to an undirected network.
	 *
	 * @param graph the network topological graph
	 * @param directed indicates if an directed matrix should be computed (true) or not (false)
	 */
	private void createDelayMatrix(final TopologicalGraph graph, final boolean directed) {
		mTotalNodeNum = graph.getNumberOfNodes();
		mDelayMatrix = Util.newSquareMatrix(mTotalNodeNum, Double.MAX_VALUE);

        for (final TopologicalLink edge : graph.getLinksList()) {
			mDelayMatrix[edge.getSrcNodeID()][edge.getDestNodeID()] = edge.getLinkDelay();
			if (!directed) {
				// according to symmetry to all communication-paths
				mDelayMatrix[edge.getDestNodeID()][edge.getSrcNodeID()] = edge.getLinkDelay();
			}
		}
	}

	/**
     * Calculates connection-delays between every pair or nodes
	 * and the shortest path between them.
	 */
	private void calculateShortestPath() {
		final FloydWarshall floyd = new FloydWarshall(mTotalNodeNum);
		mDelayMatrix = floyd.computeShortestPaths(mDelayMatrix);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(100);

		builder.append(
		    String.format("just a simple printout of the distance-aware-topology-class%ndelay-matrix is:%n"));

		for (int column = 0; column < mTotalNodeNum; ++column) {
			builder.append('\t').append(column);
		}

		for (int row = 0; row < mTotalNodeNum; ++row) {
			builder.append(System.lineSeparator()).append(row);

			for (int col = 0; col < mTotalNodeNum; ++col) {
				if (mDelayMatrix[row][col] == Double.MAX_VALUE) {
					builder.append("\t-");
				} else {
					builder.append('\t').append(mDelayMatrix[row][col]);
				}
			}
		}

		return builder.toString();
	}
}
