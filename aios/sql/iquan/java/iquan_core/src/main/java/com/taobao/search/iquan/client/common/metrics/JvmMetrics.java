package com.taobao.search.iquan.client.common.metrics;

public class JvmMetrics {
    // system
    int nproc = 0;

    // class loading
    long totalLoadedClassCount = 0;
    int loadedClassCount = 0;
    long unLoadedClassCount = 0;

    // compile
    long compileTimeMs = 0;

    // garbage collect
    long lastYoungGCCount = 0;
    long lastYoungGCTimeMs = 0;
    long recentYoungGCCount = 0;
    long recentYoungGCTimeMs = 0;

    long lastFullGCCount = 0;
    long lastFullGCTimeMs = 0;
    long recentFullGCCount = 0;
    long recentFullGCTimeMs = 0;

    // memory
    long heapUsageBytes = 0;
    long heapCommittedBytes = 0;
    long heapMaxBytes = -1; // -1表示未定义

    long nonHeapUsageBytes = 0;
    long nonHeapCommittedBytes = 0;
    long nonHeapMaxBytes = -1;

    // memory pool
    long codeCacheUsageBytes = 0;
    long codeCacheCommittedBytes = 0;
    long codeCacheMaxBytes = -1;

    long metaspaceUsageBytes = 0;
    long metaspaceCommittedBytes = 0;
    long metaspaceMaxBytes = -1;

    long compressedClassSpaceUsageBytes = 0;
    long compressedClassSpaceCommittedBytes = 0;
    long compressedClassSpaceMaxBytes = -1;

    long g1EdenSpaceUsageBytes = 0;
    long g1EdenSpaceCommittedBytes = 0;
    long g1EdenSpaceMaxBytes = -1;

    long g1SurvivorSpaceUsageBytes = 0;
    long g1SurvivorSpaceCommittedBytes = 0;
    long g1SurvivorSpaceMaxBytes = -1;

    long g1OldGenUsageBytes = 0;
    long g1OldGenCommittedBytes = 0;
    long g1OldGenMaxBytes = -1;

    // thread
    int totalThreadCount = 0;
    int daemonThreadCount = 0;
    int runnableThreadCount = 0;
}
