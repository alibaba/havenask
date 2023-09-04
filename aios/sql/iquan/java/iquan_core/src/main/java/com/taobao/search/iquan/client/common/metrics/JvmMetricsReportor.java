package com.taobao.search.iquan.client.common.metrics;

import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JvmMetricsReportor {
    private final static Logger logger = LoggerFactory.getLogger(JvmMetricsReportor.class);
    private final static String GroupName = JvmMetricsReportor.class.getSimpleName();
    private final static String PrefixName = "sql.jvm.";

    private final static String System_NProc = PrefixName + "system.nproc";

    private final static String ClassLoading_TotalLoadedCount = PrefixName + "class_loading.total_loaded_count";
    private final static String ClassLoading_LoadedCount = PrefixName + "class_loading.loaded_count";
    private final static String ClassLoading_UnLoadedCount = PrefixName + "class_loading.unloaded_count";

    private final static String Compile_CompileTimeMs = PrefixName + "compile.compile_time_ms";

    private final static String GC_YoungGCCount = PrefixName + "gc.young_count";
    private final static String GC_YoungGCTimeMs = PrefixName + "gc.young_time_ms";
    private final static String GC_FullGCCount = PrefixName + "gc.full_count";
    private final static String GC_FullGCTimeMs = PrefixName + "gc.full_time_ms";

    private final static String Memory_HeapUsageBytes = PrefixName + "memory.heap.usage_bytes";
    private final static String Memory_HeapCommittedBytes = PrefixName + "memory.heap.committed_bytes";
    private final static String Memory_HeapMaxBytes = PrefixName + "memory.heap.max_bytes";
    private final static String Memory_NonHeapUsageBytes = PrefixName + "memory.non_heap.usage_bytes";
    private final static String Memory_NonHeapCommittedBytes = PrefixName + "memory.non_heap.committed_bytes";
    private final static String Memory_NonHeapMaxBytes = PrefixName + "memory.non_heap.max_bytes";

    private final static String MemoryPool_CodeCacheUsageBytes = PrefixName + "mem_pool.code_cache.usage_bytes";
    private final static String MemoryPool_CodeCacheCommittedBytes = PrefixName + "mem_pool.code_cache.committed_bytes";
    private final static String MemoryPool_CodeCacheMaxBytes = PrefixName + "mem_pool.code_cache.max_bytes";

    private final static String MemoryPool_MetaspaceUsageBytes = PrefixName + "mem_pool.metaspace.usage_bytes";
    private final static String MemoryPool_MetaspaceCommittedBytes = PrefixName + "mem_pool.metaspace.committed_bytes";
    private final static String MemoryPool_MetaspaceMaxBytes = PrefixName + "mem_pool.metaspace.max_bytes";

    private final static String MemoryPool_CompressedClassSpaceUsageBytes = PrefixName + "mem_pool.compressed_class.usage_bytes";
    private final static String MemoryPool_CompressedClassSpaceCommittedBytes = PrefixName + "mem_pool.compressed_class.committed_bytes";
    private final static String MemoryPool_CompressedClassSpaceMaxBytes = PrefixName + "mem_pool.compressed_class.max_bytes";

    private final static String MemoryPool_G1EdenSpaceUsageBytes = PrefixName + "mem_pool.g1_eden.usage_bytes";
    private final static String MemoryPool_G1EdenSpaceCommittedBytes = PrefixName + "mem_pool.g1_eden.committed_bytes";
    private final static String MemoryPool_G1EdenSpaceMaxBytes = PrefixName + "mem_pool.g1_eden.max_bytes";

    private final static String MemoryPool_G1SurvivorSpaceUsageBytes = PrefixName + "mem_pool.g1_survivor.usage_bytes";
    private final static String MemoryPool_G1SurvivorSpaceCommittedBytes = PrefixName + "mem_pool.g1_survivor.committed_bytes";
    private final static String MemoryPool_G1SurvivorSpaceMaxBytes = PrefixName + "mem_pool.g1_survivor.max_bytes";

    private final static String MemoryPool_G1OldGenUsageBytes = PrefixName + "mem_pool.g1_old.usage_bytes";
    private final static String MemoryPool_G1OldGenCommittedBytes = PrefixName + "mem_pool.g1_old.committed_bytes";
    private final static String MemoryPool_G1OldGenMaxBytes = PrefixName + "mem_pool.g1_old.max_bytes";

    private final static String Thread_TotalCount = PrefixName + "thread.total_count";
    private final static String Thread_DaemonCount = PrefixName + "thread.daemon_count";
    private final static String Thread_RunnableCount = PrefixName + "thread.runnable_count";

    private static KMonitor kMonitor = null;

    public static boolean init() {
        kMonitor = KMonitorFactory.getKMonitor(GroupName);

        kMonitor.register(System_NProc, MetricType.GAUGE);

        kMonitor.register(ClassLoading_TotalLoadedCount, MetricType.GAUGE);
        kMonitor.register(ClassLoading_LoadedCount, MetricType.GAUGE);
        kMonitor.register(ClassLoading_UnLoadedCount, MetricType.GAUGE);

        kMonitor.register(Compile_CompileTimeMs, MetricType.GAUGE);

        kMonitor.register(GC_YoungGCCount, MetricType.GAUGE);
        kMonitor.register(GC_YoungGCTimeMs, MetricType.GAUGE);
        kMonitor.register(GC_FullGCCount, MetricType.GAUGE);
        kMonitor.register(GC_FullGCTimeMs, MetricType.GAUGE);

        kMonitor.register(Memory_HeapUsageBytes, MetricType.GAUGE);
        kMonitor.register(Memory_HeapCommittedBytes, MetricType.GAUGE);
        kMonitor.register(Memory_HeapMaxBytes, MetricType.GAUGE);
        kMonitor.register(Memory_NonHeapUsageBytes, MetricType.GAUGE);
        kMonitor.register(Memory_NonHeapCommittedBytes, MetricType.GAUGE);
        kMonitor.register(Memory_NonHeapMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_CodeCacheUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_CodeCacheCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_CodeCacheMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_MetaspaceUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_MetaspaceCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_MetaspaceMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_CompressedClassSpaceUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_CompressedClassSpaceCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_CompressedClassSpaceMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_G1EdenSpaceUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1EdenSpaceCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1EdenSpaceMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_G1SurvivorSpaceUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1SurvivorSpaceCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1SurvivorSpaceMaxBytes, MetricType.GAUGE);

        kMonitor.register(MemoryPool_G1OldGenUsageBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1OldGenCommittedBytes, MetricType.GAUGE);
        kMonitor.register(MemoryPool_G1OldGenMaxBytes, MetricType.GAUGE);

        kMonitor.register(Thread_TotalCount, MetricType.GAUGE);
        kMonitor.register(Thread_DaemonCount, MetricType.GAUGE);
        kMonitor.register(Thread_RunnableCount, MetricType.GAUGE);

        return true;
    }

    public static void destroy() {
        kMonitor = null;
    }

    public static void report(JvmMetrics metrics) {
        if (kMonitor == null) {
            return;
        }

        kMonitor.report(System_NProc, metrics.nproc);

        kMonitor.report(ClassLoading_TotalLoadedCount, metrics.totalLoadedClassCount);
        kMonitor.report(ClassLoading_LoadedCount, metrics.loadedClassCount);
        kMonitor.report(ClassLoading_UnLoadedCount, metrics.unLoadedClassCount);

        kMonitor.report(Compile_CompileTimeMs, metrics.compileTimeMs);

        kMonitor.report(GC_YoungGCCount, metrics.recentYoungGCCount);
        if (metrics.recentYoungGCCount > 0) {
            kMonitor.report(GC_YoungGCTimeMs, metrics.recentYoungGCTimeMs * 1.0 / metrics.recentYoungGCCount);
        } else {
            kMonitor.report(GC_YoungGCTimeMs, 0);
        }
        kMonitor.report(GC_FullGCCount, metrics.recentFullGCCount);
        if (metrics.recentFullGCCount > 0) {
            kMonitor.report(GC_FullGCTimeMs, metrics.recentFullGCTimeMs * 1.0 / metrics.recentFullGCCount);
        } else {
            kMonitor.report(GC_FullGCTimeMs, 0);
        }

        kMonitor.report(Memory_HeapUsageBytes, metrics.heapUsageBytes);
        kMonitor.report(Memory_HeapCommittedBytes, metrics.heapCommittedBytes);
        kMonitor.report(Memory_HeapMaxBytes, metrics.heapMaxBytes);
        kMonitor.report(Memory_NonHeapUsageBytes, metrics.nonHeapUsageBytes);
        kMonitor.report(Memory_NonHeapCommittedBytes, metrics.nonHeapCommittedBytes);
        kMonitor.report(Memory_NonHeapMaxBytes, metrics.nonHeapMaxBytes);

        kMonitor.report(MemoryPool_CodeCacheUsageBytes, metrics.codeCacheUsageBytes);
        kMonitor.report(MemoryPool_CodeCacheCommittedBytes, metrics.codeCacheCommittedBytes);
        kMonitor.report(MemoryPool_CodeCacheMaxBytes, metrics.codeCacheMaxBytes);

        kMonitor.report(MemoryPool_MetaspaceUsageBytes, metrics.metaspaceUsageBytes);
        kMonitor.report(MemoryPool_MetaspaceCommittedBytes, metrics.metaspaceCommittedBytes);
        kMonitor.report(MemoryPool_MetaspaceMaxBytes, metrics.metaspaceMaxBytes);

        kMonitor.report(MemoryPool_CompressedClassSpaceUsageBytes, metrics.compressedClassSpaceUsageBytes);
        kMonitor.report(MemoryPool_CompressedClassSpaceCommittedBytes, metrics.compressedClassSpaceCommittedBytes);
        kMonitor.report(MemoryPool_CompressedClassSpaceMaxBytes, metrics.compressedClassSpaceMaxBytes);

        kMonitor.report(MemoryPool_G1EdenSpaceUsageBytes, metrics.g1EdenSpaceUsageBytes);
        kMonitor.report(MemoryPool_G1EdenSpaceCommittedBytes, metrics.g1EdenSpaceCommittedBytes);
        kMonitor.report(MemoryPool_G1EdenSpaceMaxBytes, metrics.g1EdenSpaceMaxBytes);

        kMonitor.report(MemoryPool_G1SurvivorSpaceUsageBytes, metrics.g1SurvivorSpaceUsageBytes);
        kMonitor.report(MemoryPool_G1SurvivorSpaceCommittedBytes, metrics.g1SurvivorSpaceCommittedBytes);
        kMonitor.report(MemoryPool_G1SurvivorSpaceMaxBytes, metrics.g1SurvivorSpaceMaxBytes);

        kMonitor.report(MemoryPool_G1OldGenUsageBytes, metrics.g1OldGenUsageBytes);
        kMonitor.report(MemoryPool_G1OldGenCommittedBytes, metrics.g1OldGenCommittedBytes);
        kMonitor.report(MemoryPool_G1OldGenMaxBytes, metrics.g1OldGenMaxBytes);

        kMonitor.report(Thread_TotalCount, metrics.totalThreadCount);
        kMonitor.report(Thread_DaemonCount, metrics.daemonThreadCount);
        kMonitor.report(Thread_RunnableCount, metrics.runnableThreadCount);
    }
}
