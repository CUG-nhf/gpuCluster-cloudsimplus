package org.cloudsimplus.gpu_cluster_simulator;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Job {
    private int jobId;
    private int num_gpu;
    private double submit_time;
    private int iterations;
    private String model_name;
    private long duration;
    private int interval;

    /**
     * 表示job到达等待队列的时间，而不是提到到集群的时间
     */
    private double arrival_time = -1;

    public Job(int jobId, int gpu, int st, int its, String nm, long dt, int it){
        this.jobId = jobId;
        this.num_gpu = gpu;
        this.submit_time = st;
        this.iterations = its;
        this.model_name = nm;
        this.duration = dt;
        this.interval = it;
    }

    public Job(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job object cannot be null");
        }

        this.jobId = job.getJobId();
        this.num_gpu = job.getNum_gpu();
        this.submit_time = job.getSubmit_time();
        this.iterations = job.getIterations();
        this.model_name = job.getModel_name(); // 使用深拷贝或不变对象
        this.duration = job.getDuration();
        this.interval = job.getInterval();
        this.arrival_time = job.getArrival_time();
    }

}
