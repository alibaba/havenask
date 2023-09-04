package com.taobao.search.iquan.client.common.metrics;

import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetricsReporter {
    private final static Logger logger = LoggerFactory.getLogger(QueryMetricsReporter.class);
    private final static String GroupName = QueryMetricsReporter.class.getSimpleName();
    private final static String PrefixName = "sql.query.";

    public final static String In_Qps = PrefixName + "in.qps";
    public final static String In_Error_Qps = PrefixName + "in.error_qps";
    public final static String In_Success_Qps = PrefixName + "in.success_qps";
    public final static String In_LatencyUs = PrefixName + "in.latency_us";
    public final static String In_Success_LatencyUs = PrefixName + "in.success_latency_us";

    public final static String Parse_LatencyUs = PrefixName + "parse.latency_us";
    public final static String Catalog_Register_LatencyUs = PrefixName + "catalog_register.latency_us";
    public final static String Validate_LatencyUs = PrefixName + "validate.latency_us";
    public final static String Transform_LatencyUs = PrefixName + "transform.latency_us";
    public final static String Optimize_LatencyUs = PrefixName + "optimize.latency_us";
    public final static String Post_Optimize_LatencyUs = PrefixName + "post_optimize.latency_us";

    public final static String Cache_Hit_Qps = PrefixName + "cache_hit.qps";
    public final static String Cache_Miss_Qps = PrefixName + "cache_miss.qps";

    private static KMonitor kMonitor = null;

    public static boolean init() {
        kMonitor = KMonitorFactory.getKMonitor(GroupName);

        kMonitor.register(In_Qps, MetricType.QPS);
        kMonitor.register(In_Error_Qps, MetricType.QPS);
        kMonitor.register(In_Success_Qps, MetricType.QPS);
        kMonitor.register(In_LatencyUs, MetricType.GAUGE);
        kMonitor.register(In_Success_LatencyUs, MetricType.GAUGE);

        kMonitor.register(Parse_LatencyUs, MetricType.GAUGE);
        kMonitor.register(Catalog_Register_LatencyUs, MetricType.GAUGE);
        kMonitor.register(Validate_LatencyUs, MetricType.GAUGE);
        kMonitor.register(Transform_LatencyUs, MetricType.GAUGE);
        kMonitor.register(Optimize_LatencyUs, MetricType.GAUGE);
        kMonitor.register(Post_Optimize_LatencyUs, MetricType.GAUGE);

        kMonitor.register(Cache_Hit_Qps, MetricType.QPS);
        kMonitor.register(Cache_Miss_Qps, MetricType.QPS);

        return true;
    }

    public static void destroy() {
        kMonitor = null;
    }

    public static KMonitor getkMonitor() {
        return kMonitor;
    }
}
