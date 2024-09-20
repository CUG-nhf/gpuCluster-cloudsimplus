package org.cloudsimplus.gpu_cluster_simulator;

import org.cloudsimplus.allocationpolicies.VmAllocationPolicy;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicyBestFit;
import org.cloudsimplus.allocationpolicies.VmAllocationPolicyFirstFit;
import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.distributions.ContinuousDistribution;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import org.cloudsimplus.util.Log;
import org.cloudsimplus.distributions.UniformDistr;

import java.util.ArrayList;
import java.util.List;


public class runSimulationFromJobTrace {
    private static final String JOB_TRACE_FILE_PATH = "/home/nihaifeng/code/cloudsimplus/src/main/java/org/cloudsimplus/gpu_cluster_simulator/60_job.csv";

    private static final int  HOSTS = 2;
    private static final int  HOST_PES = 8;
    private static final long HOST_RAM = 1024*1024; //in Megabytes
    private static final long HOST_BW = 1000_000; //in Megabits/s
    private static final long HOST_STORAGE = 100_000_000; //in Megabytes


    private final CloudSimPlus simulation;
    private final DatacenterBroker broker;
    private final List<Vm> vmList = new ArrayList<>();
    private final List<Cloudlet> cloudletList = new ArrayList<>();

    private final ContinuousDistribution random =  new UniformDistr();

    public static void main(String[] args) {
        new runSimulationFromJobTrace();
    }

    private runSimulationFromJobTrace() {
        Log.setLevel(ch.qos.logback.classic.Level.WARN);

        simulation = new CloudSimPlus();

        Datacenter datacenter0 = createDatacenter();
        broker = new DatacenterBrokerSimple(simulation);
        createCloudletsFromJobTrace();
        simulation.start();

        final var cloudletFinishedList = broker.getCloudletSubmittedList();
        new CloudletsTableBuilder(cloudletFinishedList).build();
    }

    private Datacenter createDatacenter() {
        final var hostList = new ArrayList<Host>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            final var host = createHost();
            hostList.add(host);
        }

        VmAllocationPolicy policy = new VmAllocationPolicyFirstFit();

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

    private void createCloudletsFromJobTrace() {
        List<Job> jobs = new ReadJobTrace(JOB_TRACE_FILE_PATH).getJobs();;
        for (Job job : jobs) {
            createCloudlet(job);
        }
    }

    private void createCloudlet(Job job) {
        // 1. 创建作业
        var job_id = job.getJobId();
        var job_submit = job.getSubmit_time();
        int job_gpu_num = job.getNum_gpu();
        long job_duration = job.getDuration();
        final var cloudlet = new CloudletSimple(job_id, job_duration, job_gpu_num);
        cloudlet.addOnFinishListener(this::whenCloudletFinished);
        cloudletList.add(cloudlet);

        // 2. 为这个作业创建Vm
        final var vm = new VmSimple(job_id, 1, job_gpu_num);
        vm.setRam(512).setBw(1000).setSize(10_000);
        vm.setSubmissionDelay(job_submit);
        vmList.add(vm);

        broker.bindCloudletToVm(cloudlet, vm);
        broker.submitCloudlet(cloudlet);
        broker.submitVm(vm);
    }

    // 如果job完成，则删除job所在的虚拟机
    private void whenCloudletFinished(CloudletVmEventInfo event) {
        var vm = event.getCloudlet().getVm();
        vm.shutdown();
    }
}
