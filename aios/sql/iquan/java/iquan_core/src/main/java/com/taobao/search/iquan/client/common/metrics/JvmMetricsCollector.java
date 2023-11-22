package com.taobao.search.iquan.client.common.metrics;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JvmMetricsCollector {
    private final static Logger logger = LoggerFactory.getLogger(JvmMetricsCollector.class);
    private final static Set<String> youngGCNames =
            ImmutableSet.of("Copy", "ParNew", "PS Scavenge", "G1 Young Generation");
    private final static Set<String> fullGCNames =
            ImmutableSet.of("MarkSweepCompact", "PS MarkSweep", "ConcurrentMarkSweep", "G1 Old Generation");

    private final static OperatingSystemMXBean operatingSystemMXBean;
    private final static RuntimeMXBean runtimeMXBean;
    private final static String pid;
    private final static ClassLoadingMXBean classLoadingMXBean;
    private final static CompilationMXBean compilationMXBean;
    private final static MemoryMXBean memoryMXBean;
    private final static ThreadMXBean threadMXBean;
    private final static JvmMetrics metrics = new JvmMetrics();
    private static GarbageCollectorMXBean youngGCMXBean = null;
    private static GarbageCollectorMXBean fullGCMXBean = null;
    private static MemoryPoolMXBean codeCacheMXBean = null;
    private static MemoryPoolMXBean metaspaceMXBean = null;
    private static MemoryPoolMXBean compressedClassSpaceMXBean = null;
    private static MemoryPoolMXBean g1EdenSpaceMXBean = null;
    private static MemoryPoolMXBean g1SurvivorSpaceMXBean = null;
    private static MemoryPoolMXBean g1OldGenMXBean = null;

    static {
        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        pid = runtimeMXBean.getName().split("@")[0];
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        compilationMXBean = ManagementFactory.getCompilationMXBean();
        logger.info("get compilation bean [{}] success.", compilationMXBean.getName());

        List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcBean : garbageCollectorMXBeans) {
            if (!gcBean.isValid()) {
                logger.info("garbage collector bean [{}] is not valid, skip.", gcBean.getName());
                continue;
            }
            if (youngGCNames.contains(gcBean.getName()) && youngGCMXBean == null) {
                logger.info("get young garbage collector bean [{}] success.", gcBean.getName());
                youngGCMXBean = gcBean;
                continue;
            }
            if (fullGCNames.contains(gcBean.getName()) && fullGCMXBean == null) {
                logger.info("get full garbage collector bean [{}] success.", gcBean.getName());
                fullGCMXBean = gcBean;
                continue;
            }
            logger.warn("garbage collector bean [{}] is unknown, skip.", gcBean.getName());
        }

        memoryMXBean = ManagementFactory.getMemoryMXBean();
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean poolBean : memoryPoolMXBeans) {
            if (!poolBean.isValid()) {
                logger.info("memory pool bean [{}] is not valid, skip.", poolBean.getName());
                continue;
            }
            String memoryPoolName = poolBean.getName();
            switch (memoryPoolName) {
                case "Code Cache":
                    logger.info("get code cache bean [{}] success.", poolBean.getName());
                    codeCacheMXBean = poolBean;
                    break;
                case "Metaspace":
                    logger.info("get metaspace bean [{}] success.", poolBean.getName());
                    metaspaceMXBean = poolBean;
                    break;
                case "Compressed Class Space":
                    logger.info("get compressed class space bean [{}] success.", poolBean.getName());
                    compressedClassSpaceMXBean = poolBean;
                    break;
                case "G1 Eden Space":
                    logger.info("get g1 eden space bean [{}] success.", poolBean.getName());
                    g1EdenSpaceMXBean = poolBean;
                    break;
                case "G1 Survivor Space":
                    logger.info("get g1 survivor space bean [{}] success.", poolBean.getName());
                    g1SurvivorSpaceMXBean = poolBean;
                    break;
                case "G1 Old Gen":
                    logger.info("get g1 old gen bean [{}] success.", poolBean.getName());
                    g1OldGenMXBean = poolBean;
                    break;
                default:
                    logger.warn("memory pool bean [{}] is unknown, skip.", poolBean.getName());
                    break;
            }
        }
        threadMXBean = ManagementFactory.getThreadMXBean();
    }

    public static String getPid() {
        return pid;
    }

    public static JvmMetrics getJvmMetrics() {
        return metrics;
    }

    public static void collect() {
        collectSystem();
        collectClassLoading();
        collectCompile();
        collectGarbageCollect();
        collectMemory();
        collectMemoryPool();
        collectThread();
    }

    private static void collectSystem() {
        metrics.nproc = operatingSystemMXBean.getAvailableProcessors();
    }

    private static void collectClassLoading() {
        metrics.totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        metrics.loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        metrics.unLoadedClassCount = classLoadingMXBean.getUnloadedClassCount();
    }

    private static void collectCompile() {
        if (!compilationMXBean.isCompilationTimeMonitoringSupported()) {
            return;
        }
        metrics.compileTimeMs = compilationMXBean.getTotalCompilationTime();
    }

    private static void collectGarbageCollect() {
        if (youngGCMXBean != null) {
            long youngGCCount = youngGCMXBean.getCollectionCount();
            long youngGCTimeMs = youngGCMXBean.getCollectionTime();
            metrics.recentYoungGCCount = youngGCCount - metrics.lastYoungGCCount;
            metrics.recentYoungGCTimeMs = youngGCTimeMs - metrics.lastYoungGCTimeMs;
            metrics.lastYoungGCCount = youngGCCount;
            metrics.lastYoungGCTimeMs = youngGCTimeMs;
        }

        if (fullGCMXBean != null) {
            long fullGCCount = fullGCMXBean.getCollectionCount();
            long fullGCTimeMs = fullGCMXBean.getCollectionTime();
            metrics.recentFullGCCount = fullGCCount - metrics.lastFullGCCount;
            metrics.recentFullGCTimeMs = fullGCTimeMs - metrics.lastFullGCTimeMs;
            metrics.lastFullGCCount = fullGCCount;
            metrics.lastFullGCTimeMs = fullGCTimeMs;
        }
    }

    private static void collectMemory() {
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        metrics.heapUsageBytes = heapMemoryUsage.getUsed();
        metrics.heapCommittedBytes = heapMemoryUsage.getCommitted();
        metrics.heapMaxBytes = heapMemoryUsage.getMax();

        MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
        metrics.nonHeapUsageBytes = nonHeapMemoryUsage.getUsed();
        metrics.nonHeapCommittedBytes = nonHeapMemoryUsage.getCommitted();
        metrics.nonHeapMaxBytes = nonHeapMemoryUsage.getMax();
    }

    private static void collectMemoryPool() {
        if (codeCacheMXBean != null) {
            MemoryUsage codeCacheUsage = codeCacheMXBean.getUsage();
            metrics.codeCacheUsageBytes = codeCacheUsage.getUsed();
            metrics.codeCacheCommittedBytes = codeCacheUsage.getCommitted();
            metrics.codeCacheMaxBytes = codeCacheUsage.getMax();
        }

        if (metaspaceMXBean != null) {
            MemoryUsage metaspaceUsage = metaspaceMXBean.getUsage();
            metrics.metaspaceUsageBytes = metaspaceUsage.getUsed();
            metrics.metaspaceCommittedBytes = metaspaceUsage.getCommitted();
            metrics.metaspaceMaxBytes = metaspaceUsage.getMax();
        }

        if (compressedClassSpaceMXBean != null) {
            MemoryUsage compressedClassSpaceUsage = compressedClassSpaceMXBean.getUsage();
            metrics.compressedClassSpaceUsageBytes = compressedClassSpaceUsage.getUsed();
            metrics.compressedClassSpaceCommittedBytes = compressedClassSpaceUsage.getCommitted();
            metrics.compressedClassSpaceMaxBytes = compressedClassSpaceUsage.getMax();
        }

        if (g1EdenSpaceMXBean != null) {
            MemoryUsage g1EdenSpaceUsage = g1EdenSpaceMXBean.getUsage();
            metrics.g1EdenSpaceUsageBytes = g1EdenSpaceUsage.getUsed();
            metrics.g1EdenSpaceCommittedBytes = g1EdenSpaceUsage.getCommitted();
            metrics.g1EdenSpaceMaxBytes = g1EdenSpaceUsage.getMax();
        }

        if (g1SurvivorSpaceMXBean != null) {
            MemoryUsage g1SurvivorSpaceUsage = g1SurvivorSpaceMXBean.getUsage();
            metrics.g1SurvivorSpaceUsageBytes = g1SurvivorSpaceUsage.getUsed();
            metrics.g1SurvivorSpaceCommittedBytes = g1SurvivorSpaceUsage.getCommitted();
            metrics.g1SurvivorSpaceMaxBytes = g1SurvivorSpaceUsage.getMax();
        }

        if (g1OldGenMXBean != null) {
            MemoryUsage g1OldGenUsage = g1OldGenMXBean.getUsage();
            metrics.g1OldGenUsageBytes = g1OldGenUsage.getUsed();
            metrics.g1OldGenCommittedBytes = g1OldGenUsage.getCommitted();
            metrics.g1OldGenMaxBytes = g1OldGenUsage.getMax();
        }
    }

    private static void collectThread() {
        metrics.totalThreadCount = threadMXBean.getThreadCount();
        metrics.daemonThreadCount = threadMXBean.getDaemonThreadCount();

        int runnableCount = 0;
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds());
        if (threadInfos != null) {
            for (ThreadInfo threadInfo : threadInfos) {
                if (threadInfo == null) {
                    continue;
                }
                String threadName = threadInfo.getThreadName();
                Thread.State state = threadInfo.getThreadState();
                boolean isInNative = threadInfo.isInNative();
                if (threadName.startsWith("Thread-")
                        && state == Thread.State.RUNNABLE
                        && isInNative) {
                    runnableCount++;
                }
            }
        }
        metrics.runnableThreadCount = runnableCount;
    }
}
