/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-28 14:32
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_KMONITORFACTORY_H_
#define KMONITOR_CLIENT_KMONITORFACTORY_H_

#include <map>
#include <string>

#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsConfig;
class KMonitor;
class KMonitorWorker;
class MetricsTags;

class KMonitorFactory {
public:
    static bool Init(const std::string &config_content);
    static bool Init(const MetricsConfig &config);
    static bool formatMetricsConfig(const std::string &config_str, MetricsConfig &config);
    static void Start();
    static bool IsStarted();
    static void Shutdown();
    static void SetServiceName(const std::string &name);
    static const std::string &ServiceName();
    static KMonitor *GetKMonitor(const std::string &name, bool useMetricCache = true, bool useConfigTags = true);
    static MetricsConfig *GetConfig();
    static void ReleaseKMonitor(const std::string &name);
    static std::map<std::string, KMonitor *> *GetKMonitorMap();
    static KMonitorWorker *GetWorker();
    static void registerBuildInMetrics(MetricsTags *tags = NULL, const std::string &metric_name_prefix = "");

private:
    KMonitorFactory();
    ~KMonitorFactory();
    KMonitorFactory(const KMonitorFactory &);
    KMonitorFactory &operator=(const KMonitorFactory &);

private:
    static KMonitorWorker *worker_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_KMONITORFACTORY_H_
