package org.cloudsimplus.gpu_cluster_simulator;

import org.cloudsimplus.cloudlets.Cloudlet;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class fileWriter {

    public static void writeCloudletExecutionTimesToFile(List<Cloudlet> cloudlets, String fileName) {
        try (FileWriter writer = new FileWriter(fileName)) {
            for (Cloudlet cloudlet : cloudlets) {
                double executionTime = cloudlet.getTotalExecutionTime();
                writer.write(executionTime + " ");
            }
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}
