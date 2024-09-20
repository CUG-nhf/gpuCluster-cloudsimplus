package org.cloudsimplus.gpu_cluster_simulator;

import java.util.List;


/**
 * 从 job_queue中选择出一个要执行的job
 */
public class JobScheduler {

    // 定义枚举类型 JobSelectionPolicy
    public enum JobSelectionPolicy {
        FIFO,
        SJF
    }

    private JobSelectionPolicy jobSelectionPolicy;

    public JobScheduler(JobSelectionPolicy policy) {
        this.jobSelectionPolicy = policy;
    }

    public Job selectJob(List<Job> job_queue) {
        return switch (jobSelectionPolicy) {
            case FIFO -> processFifo(job_queue);
            case SJF -> processSjf(job_queue);
            default -> throw new IllegalArgumentException("Unsupported job selection policy: " + jobSelectionPolicy);
        };
    }

    private Job processFifo(List<Job> job_queue) {
        if (!job_queue.isEmpty()) {
            return job_queue.remove(0);
        }
        return null;
    }

    private Job processSjf(List<Job> job_queue) {
        // 从 job_queue 中选出 job.getDuration() 最小的 job
        Job minDurationJob = null;
        double minDuration = Double.MAX_VALUE;
        for (Job job : job_queue) {
            if (job.getDuration() < minDuration) {
                minDuration = job.getDuration();
                minDurationJob = job;
            }
        }
        job_queue.remove(minDurationJob);
        return minDurationJob;
    }
}
