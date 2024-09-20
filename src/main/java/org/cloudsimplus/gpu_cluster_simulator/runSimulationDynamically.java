package org.cloudsimplus.gpu_cluster_simulator;

import org.cloudsimplus.allocationpolicies.*;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.distributions.UniformDistr;
import org.cloudsimplus.distributions.PoissonDistr;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.VmDatacenterEventInfo;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.util.Log;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;

import java.util.ArrayList;
import java.util.List;


public class runSimulationDynamically {
    private static final String JOB_TRACE_FILE_PATH = "/home/nihaifeng/code/cloudsimplus/src/main/java/org/cloudsimplus/gpu_cluster_simulator/60_job.csv";

    private static final long TIME_TO_TERMINATE_SIMULATION = 24*3600;  // in seconds
    private static final long JOB_QUEUE_SCHEDULING_INTERVAL_SECONDS = 60;
    private static final int JOB_NUMBER_PER_SCHEDULING_INTERVAL = 2;

    private static final String JOB_SELECTION_POLICY = "FIFO";
//    private static final String JOB_SELECTION_POLICY = "SJF";

    private static final int  HOSTS = 10;
    private static final int  HOST_PES = 8;
    private static final long HOST_RAM = 1024*1024; //in Megabytes
    private static final long HOST_BW = 1000_000; //in Megabits/s
    private static final long HOST_STORAGE = 100_000_000; //in Megabytes


    private final CloudSimPlus simulation;
    private final DatacenterBroker broker;
    private final List<Cloudlet> cloudletList = new ArrayList<>();
    private final List<Job> jobs = getAllJobs(JOB_TRACE_FILE_PATH);

    private final UniformDistr uniform =  new UniformDistr(112717614L);
    private final PoissonDistr poisson = new PoissonDistr(0.01, 11271713L);
    private double nextJobArrivalTime = 0;
    private long total_finished_job_num = 0;
    private final List<Job> job_queue = new ArrayList<>();

    public static void main(String[] args) {
        new runSimulationDynamically();
    }

    private runSimulationDynamically() {
        Log.setLevel(ch.qos.logback.classic.Level.WARN);

        simulation = new CloudSimPlus();
        simulation.terminateAt(TIME_TO_TERMINATE_SIMULATION);
        simulation.addOnClockTickListener(this::createRandomCloudlets);

        Datacenter datacenter0 = createDatacenter();
        broker = new DatacenterBrokerSimple(simulation);
        simulation.start();

        final var cloudletFinishedList = broker.getCloudletFinishedList();
        new CloudletsTableBuilder(broker.getCloudletFinishedList()).build();
        System.out.println("Total finished Job number: " + total_finished_job_num);

        double sum = cloudletFinishedList.stream().mapToDouble(Cloudlet::getJCT).sum();
        System.out.printf("Average JCT: %.2f\n", sum / cloudletFinishedList.size());

    }

    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            final var host = createHost();
            hostList.add(host);
        }

        // 作业部署到哪台服务器上的算法，在这里修改
        VmAllocationPolicy policy = new VmAllocationPolicyFirstFit();
//        VmAllocationPolicy policy = new VmAllocationPolicyBestFit();
//        VmAllocationPolicy policy = new VmAllocationPolicyRandom(new UniformDistr(19991202L));
//        VmAllocationPolicy policy = new VmAllocationPolicyRoundRobin();

        Datacenter dc = new DatacenterSimple(simulation, hostList, policy);
        dc.setSchedulingInterval(1);
        dc.setHostSearchRetryDelay(60);

        return dc;
    }

    private Host createHost() {
        final var peList = new ArrayList<Pe>(HOST_PES);
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(1));
        }
        return new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
    }

    private List<Job> getAllJobs(String filePath) {
        return new ReadJobTrace(filePath).getJobs();
    }

    private void createCloudlet(Job job) {
        // 1. 创建作业
        var job_id = job.getJobId();
        int job_gpu_num = job.getNum_gpu();
        long job_duration = job.getDuration();
        final var cloudlet = new CloudletSimple(job_id, job_duration, job_gpu_num);
        cloudlet.addOnFinishListener(this::whenCloudletFinished);
        // 为cloudlet添加属性，arrival time表示作业达到等待队列的时间，后期用于计算作业再等待队列中的等待时间
        cloudlet.setGpuJob(job);
        cloudletList.add(cloudlet);

        // 2. 为这个作业创建Vm
        final Vm vm = new VmSimple(job_id, 1, job_gpu_num);
        vm.setRam(512).setBw(1000).setSize(10_000);
        vm.addOnCreationFailureListener(this::whenVmCreationFailed);

        broker.bindCloudletToVm(cloudlet, vm);
        broker.submitCloudlet(cloudlet);
        broker.submitVm(vm);
    }

    // 如果job完成，则删除job所在的虚拟机
    private void whenCloudletFinished(CloudletVmEventInfo event) {
        var vm = event.getCloudlet().getVm();
        vm.shutdown();
        total_finished_job_num ++;
    }

    // 如果Vm创建失败，则把job收回，重新加入到job_queue
    private void whenVmCreationFailed(VmDatacenterEventInfo event) {
        Job job = broker.withdrawJobFromWaitingList(event.getVm());
        job_queue.add(job);
    }
    /**
     *  在这个函数里可以实现从作业队列里选择作业提交的算法
     */
    private void selectJobFromQueueToExecute() {
        if (job_queue.isEmpty()) {
            return;
        }
        if (JOB_SELECTION_POLICY == "FIFO") {
            createCloudlet(job_queue.get(0));
            job_queue.remove(0);
        }
        if (JOB_SELECTION_POLICY == "SJF") {
            Job minDurationJob = null;
            double minDuration = Double.MAX_VALUE;
            for (Job job : job_queue) {
                if (job.getDuration() < minDuration) {
                    minDuration = job.getDuration();
                    minDurationJob = job;
                }
            }

            if (minDurationJob != null) {
                createCloudlet(minDurationJob);
                job_queue.remove(minDurationJob);
            }
        }

    }

    private void generateJobToQueue(final double currentTime) {
        int jobIndex = (int) (uniform.sample() * jobs.size());
        Job tmp_job = new Job(jobs.get(jobIndex));
        tmp_job.setArrival_time(currentTime);
        job_queue.add(tmp_job);
        nextJobArrivalTime = poisson.sample() + currentTime;
    }

    private void createRandomCloudlets(EventInfo eventInfo) {
        if ((int)eventInfo.getTime() == (int) nextJobArrivalTime) {
            int n = 2;
            while (n > 0) {
                generateJobToQueue(eventInfo.getTime());
                n --;
            }
        }
        if ((int) eventInfo.getTime() % JOB_QUEUE_SCHEDULING_INTERVAL_SECONDS == 0) {
//            System.out.println(job_queue.size());
            for (var i = 0; i < JOB_NUMBER_PER_SCHEDULING_INTERVAL; i ++) {
                selectJobFromQueueToExecute();
            }
        }
    }
}
