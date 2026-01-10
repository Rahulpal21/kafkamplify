package com.github.rahulpal21.kafkamplify.scaling;

import com.sun.management.OperatingSystemMXBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ProcessMonitor {

    private OperatingSystemMXBean osBean = (OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();

    @Scheduled(fixedDelay = 2000)
    public void watch() {
        // Implementation for watching processes
        System.out.println("----- Process Monitor -----");
        System.out.println("Process CPU Load: " + osBean.getProcessCpuLoad());
        System.out.println("System CPU Load: " + osBean.getSystemCpuLoad());
        System.out.println("Total Physical Memory: " + osBean.getTotalPhysicalMemorySize());
        System.out.println("Free Physical Memory: " + osBean.getFreePhysicalMemorySize());
        System.out.println("Committed Virtual Memory: " + osBean.getCommittedVirtualMemorySize());
        System.out.println("-----------------------------------");
    }
}
