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
import org.cloudsimplus.distributions.ContinuousDistribution;
import org.cloudsimplus.distributions.DiscreteDistribution;
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

    private static final JobScheduler jobSelectionPolicy = new JobScheduler(JobScheduler.JobSelectionPolicy.SJF);

    private static final int  HOSTS = 10;
    private static final int  HOST_PES = 8;
    private static final long HOST_RAM = 1024*1024; //in Megabytes
    private static final long HOST_BW = 1000_000; //in Megabits/s
    private static final long HOST_STORAGE = 100_000_000; //in Megabytes


    private final CloudSimPlus simulation;
    private final DatacenterBroker broker;
    private int cloudletID = 0;

    private final UniformDistr jobTypeGenerator =  new UniformDistr(112717614L);
    private final PoissonDistr jobArriveTimeGenerator = new PoissonDistr(0.01, 11271713L);
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
        simulation.addOnClockTickListener(this::simulationTimeListener);

        Datacenter datacenter0 = createDatacenter();
        broker = new DatacenterBrokerSimple(simulation);
        simulation.start();


//        new CloudletsTableBuilder(broker.getCloudletSubmittedList()).build();
        System.out.println("Total finished Job number: " + total_finished_job_num);
        final var cloudletFinishedList = broker.getCloudletFinishedList();
        double sum = cloudletFinishedList.stream().mapToDouble(Cloudlet::getJCT).sum();
        System.out.printf("Average JCT: %.2f\n", sum / cloudletFinishedList.size());

    }

    // 从trace中获取所有的job类型，后期可能自定义job总类，所以这里先写个函数获取总jobs
    private List<Job> getAllJobs() {
        return new ReadJobTrace(JOB_TRACE_FILE_PATH).getJobs();
    }

    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            final var host = createHost();
            hostList.add(host);
        }

        // 作业部署算法
        // TODO 在cloudsim plus中放置和整理是一个整体，都在migrationPolicy中，但是在我们的gpu碎片整理设计中，这俩是分开的；下一步是想办法适配
//        VmAllocationPolicy policy = new VmAllocationPolicyFirstFit();
//        VmAllocationPolicy policy = new VmAllocationPolicyBestFit();
//        VmAllocationPolicy policy = new VmAllocationPolicyRandom(new UniformDistr(19991202L));
        VmAllocationPolicy policy = new VmAllocationPolicyRoundRobin();

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

    private void createCloudlet(Job job) {
        // 1. 创建作业
        var job_id = job.getJobId();
        int job_gpu_num = job.getNum_gpu();
        long job_duration = job.getDuration();
        final var cloudlet = new CloudletSimple(cloudletID, job_duration, job_gpu_num);
        cloudlet.addOnFinishListener(this::whenCloudletFinished);
        // 为cloudlet添加属性，arrival time表示作业达到等待队列的时间，后期用于计算作业再等待队列中的等待时间
        cloudlet.setGpuJob(job);

        // 2. 为这个作业创建Vm
        final Vm vm = new VmSimple(cloudletID, 1, job_gpu_num);
        vm.setRam(512).setBw(1000).setSize(10_000);
        vm.addOnCreationFailureListener(this::whenVmCreationFailed);

        broker.bindCloudletToVm(cloudlet, vm);
        broker.submitCloudlet(cloudlet);
        broker.submitVm(vm);

        cloudletID ++;
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
     * 随机生成一个具体的作业，并加入到job_queue中
     * @param currentTime 作业到达时间
     * @param jobArriveTimeGenerator 作业到达时间随机生成器，用于生产下一次作业到达间隔
     * @param jobTypeGenerator 作业类型随机生成器，在所有作业类型（例如60_job.csv）中选择一个具体的作业，作为此次达到的作业
     */
    private void generateJobToQueue(final double currentTime,
                                    DiscreteDistribution jobArriveTimeGenerator,
                                    ContinuousDistribution jobTypeGenerator) {
        final List<Job> all_jobs = getAllJobs();

        int jobIndex = (int) (jobTypeGenerator.sample() * all_jobs.size());
        Job tmp_job = new Job(all_jobs.get(jobIndex));
        tmp_job.setArrival_time(currentTime);
        job_queue.add(tmp_job);
        nextJobArrivalTime = jobArriveTimeGenerator.sample() + currentTime;
    }

    private void simulationTimeListener(EventInfo eventInfo) {
        // 1. 如果达到nextJobArrivalTime时，生产新的到达Job
        if ((int)eventInfo.getTime() == (int) nextJobArrivalTime) {
            int n = 4;
            while (n > 0) {
                generateJobToQueue(eventInfo.getTime(),jobArriveTimeGenerator,jobTypeGenerator);
                n --;
            }
        }

        // 2. 如果时间到达调度间隔，则从job_queue中选择作业提交到集群
        if ((int) eventInfo.getTime() % JOB_QUEUE_SCHEDULING_INTERVAL_SECONDS == 0) {
            for (var i = 0; i < JOB_NUMBER_PER_SCHEDULING_INTERVAL; i ++) {
                // 执行作业选择算法，从job_queue中选择作业然后提交
                Job job = jobSelectionPolicy.selectJob(job_queue);
                if (job != null) {
                    createCloudlet(job);
                }
            }
        }
    }
}
