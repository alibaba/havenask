/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-03-09 11:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_KMONITORWORKER_H_
#define KMONITOR_CLIENT_KMONITORWORKER_H_

#include <map>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsSystem;
class MetricsConfig;
class KMonitor;
class MetricsTags;
class Sink;
TYPEDEF_PTR(Sink);

class KMonitorWorker {
public:
    KMonitorWorker();
    ~KMonitorWorker();
    bool Init(const std::string &config_content);
    bool Init(const MetricsConfig &config);
    void Start();
    bool IsStarted();
    void Shutdown();
    void SetServiceName(const std::string &name);
    const std::string &ServiceName();
    KMonitor *GetKMonitor(const std::string &name, bool useMetricCache = true, bool useConfigTags = true);
    MetricsConfig *GetConfig() { return config_; }
    void ReleaseKMonitor(const std::string &name);
    std::map<std::string, KMonitor *> *GetKMonitorMap();
    void registerBuildInMetrics(MetricsTags *tags, const std::string &metric_name_prefix = "");

public:
    SinkPtr GetSink(const std::string &name) const;
    void addCommonTags();

private:
    void EscapeString(std::string &input);

private:
    KMonitorWorker(const KMonitorWorker &);
    KMonitorWorker &operator=(const KMonitorWorker &);
    MetricsSystem *getMetricsSystem() { return system_; }

private:
    std::string name_;
    bool started_ = false;
    MetricsSystem *system_ = nullptr;
    MetricsConfig *config_ = nullptr;
    std::map<std::string, KMonitor *> *kmonitor_map_ = nullptr;
    autil::ThreadMutex metric_mutex_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_KMONITORWORKER_H_
