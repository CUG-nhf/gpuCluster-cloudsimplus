package org.cloudsimplus.gpu_cluster_simulator;

import lombok.Getter;
import lombok.Setter;
import org.cloudsimplus.util.MathUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadJobTrace {
    private String csvFile = "path/to/your/file.csv"; // CSV文件路径

    public ReadJobTrace(String filePath) {
        csvFile = filePath;
    }

    public List<String[]> csvReader(String filePath) {
        List<String[]> tmp_data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                tmp_data.add(fields); // 添加到列表
            }
        } catch (IOException e) {
            e.printStackTrace(); // 打印异常信息
        }

        return tmp_data;
    }

    // 从csv文件中读取min(job_num, len(csv)) 个job对象
    public List<Job> getJobs() {
        List<String[]> csv_context = csvReader(csvFile);

        List<Job> jobList = new ArrayList<>();
        for (int i = 1; i < csv_context.size(); ++ i) {
            String[] data = csv_context.get(i);
            jobList.add(new Job(
                Integer.parseInt(data[0]),
                Integer.parseInt(data[1]),
                Integer.parseInt(data[2]),
                Integer.parseInt(data[3]),
                data[4],
                Long.parseLong(data[5]),
                Integer.parseInt(data[6]))
            );
        }
        return jobList;
    }
}

