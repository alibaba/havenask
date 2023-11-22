package com.taobao.search.iquan.client.common.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.core.MetricsTags;
import com.taobao.kmonitor.impl.KMonitorConfig;
import com.taobao.search.iquan.client.common.json.api.JsonKMonConfig;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ClientUtils;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanKMonService {
    private static final Logger logger = LoggerFactory.getLogger(IquanKMonService.class);
    private static final AtomicBoolean bInit = new AtomicBoolean(false);
    private static ScheduledExecutorService reportThread = null;

    public static synchronized boolean isStart() {
        return bInit.get();
    }

    public static synchronized byte[] start(JsonKMonConfig config) {
        SqlResponse response = new SqlResponse();

        if (!bInit.compareAndSet(false, true)) {
            logger.warn("kmonitor has been started.");
            return ClientUtils.toResponseByte(response);
        }

        try {
            logger.info("kmonitor start begin.");
            logger.info("kmonitor config: {}", IquanRelOptUtils.toPrettyJson(config));
            KMonitorConfig.setKMonitorServiceName(config.serviceName);
            KMonitorConfig.setKMonitorTenantName(config.tenantName);
            KMonitorConfig.setSinkAddress(config.flumeAddress);
            KMonitorConfig.setAutoRecycle(config.autoRecycle);

            JvmMetricsCollector.collect();
            String pid = JvmMetricsCollector.getPid();
            MetricsTags tags = parseGlobalTags(config.globalTags, pid);
            KMonitorFactory.addGlobalTags(tags);

            KMonitorFactory.start();
            JvmMetricsReportor.init();
            QueryMetricsReporter.init();

            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("metric-report-pool-%d").build();
            reportThread = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
            reportThread.scheduleWithFixedDelay(
                    IquanKMonService::jvmReport,
                    0,
                    1000,
                    TimeUnit.MILLISECONDS);
            logger.info("kmonitor start success.");
        } catch (Exception ex) {
            logger.error("kmonitor start fail.", ex);
            response = new SqlResponse();
            ErrorUtils.setException(ex, response);
        }
        return ClientUtils.toResponseByte(response);
    }

    public static synchronized void stop() {
        if (bInit.compareAndSet(true, false)) {
            try {
                logger.info("kmonitor stop begin.");
                reportThread.shutdown();
                try {
                    if (!reportThread.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                        reportThread.shutdownNow();
                    }
                    if (!reportThread.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                        logger.error("shutdown kmonitor report thread fail");
                    }
                } catch (InterruptedException ex) {
                    reportThread.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                JvmMetricsReportor.destroy();
                QueryMetricsReporter.destroy();
                KMonitorFactory.stop();
                logger.info("kmonitor stop success.");
            } catch (Exception ex) {
                logger.error("kmonitor stop fail.", ex);
            }
        } else {
            logger.warn("kmonitor has not been started.");
        }
    }

    private static MetricsTags parseGlobalTags(String content, String pid) {
        Map<String, String> tags = new HashMap<>();
        if (pid != null && !pid.isEmpty()) {
            logger.info("kmonitor: add global tag {} = {}", "pid", pid);
            tags.put("pid", pid);
        }

        if (content != null && !content.isEmpty()) {
            String[] tagKvStrList = content.split(";");
            for (String tagKvStr : tagKvStrList) {
                if (tagKvStr == null || tagKvStr.isEmpty()) {
                    continue;
                }
                String[] tagKv = tagKvStr.split(":");
                if (tagKv.length != 2) {
                    logger.warn("kmonitor: invalid global tag {}", tagKvStr);
                    continue;
                }
                String key = tagKv[0].trim();
                String value = tagKv[1].trim();
                if (!key.isEmpty() && !value.isEmpty()) {
                    logger.info("kmonitor: add global tag {} = {}", key, value);
                    tags.put(key, value);
                } else {
                    logger.warn("kmonitor: invalid global tag {}, key/value should not be empty", tagKvStr);
                }
            }
        }
        return new ImmutableMetricTags(tags);
    }

    private static void jvmReport() {
        JvmMetricsCollector.collect();
        JvmMetrics metrics = JvmMetricsCollector.getJvmMetrics();
        JvmMetricsReportor.report(metrics);
    }
}
