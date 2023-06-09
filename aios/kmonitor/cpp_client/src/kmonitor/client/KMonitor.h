/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 11:40
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * modify:
    1. 2019-08-09 add cache
    2. 2020-02-06 depart metric and tags to improve perf, refer issue: xxxx://invalid/monitor_service/kmonitor-client-cpp/issues/86028
 * */

#ifndef KMONITOR_CLIENT_KMONITOR_H_
#define KMONITOR_CLIENT_KMONITOR_H_

#include <set>
#include <string>
#include "autil/Log.h"
#include "kmonitor/client/core/MetricsSource.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/metric/Metric.h"
#include "kmonitor/client/metric/BuildInMetrics.h"
#include "kmonitor/client/core/MetricsCache.h"
#include "kmonitor/client/core/MetricsTagsManager.h"
#include <map>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsConfig;
class MetricsManager;
class MetricsCollector;
class MutableMetric;

class KMonitor : public MetricsSource {
 public:
    KMonitor();
    ~KMonitor();
    explicit KMonitor(const std::string &name, bool useMetricCache = true);
    void SetConfig(MetricsConfig *config, bool useConfigTags = true);
    void SetServiceName(const std::string& newServiceName); // 覆盖全局的service_name
    void Register(const std::string &name, MetricType type, MetricLevel level = NORMAL);
    void Unregister(const std::string &name);

    MutableMetric* RegisterMetric(const std::string &name, MetricType type, MetricLevel level = NORMAL);
    void Report(const std::string &name, double value);
    void Report(const std::string &name, const MetricsTags* tags, double value);
    void Report(const std::string &name, const MetricsTagsPtr& tags, double value);

    const std::string& Name() override;
    void Stop();
    void GetMetrics(MetricsCollector *collector, const std::set<MetricLevel>& levels, int64_t curTimeMs) override;

    // if tags changed or use AddGlobalTag, should DeclareMetric again, and use new Metric.
    // don't free KMonitor if holding Metric outside
    Metric* DeclareMetric(const std::string &name, const MetricsTags *tags);
    Metric* DeclareMetric(MutableMetric* mutableMetric, const MetricsTags *tags);

    //destroy Metric for above two DeclareMetric, can't use metric after UndeclareMetric
    bool UndeclareMetric(Metric* metric);

    // 增加metric_name_prefix，解决以前amonitor走converter时加的metric前缀
    void registerBuildInMetrics(MetricsTags *tags, MetricLevel level = NORMAL, const std::string& metric_name_prefix = "");

    // avoid to construct Metric every Report，user can call metric->Update(value) dirctly
    void Report(Metric* metric, double value) {
        if (metric != NULL) {
            metric->Update(value);
        }
    }

    // 增加kmonitor级别的tag
    void AddTag(const std::string &k, const std::string &v) {
        tags_->AddTag(k, v);
    }

    MetricsTagsPtr GetMetricsTags(const std::map<std::string, std::string>& tags_map) {
        return tags_manager_->GetMetricsTags(tags_map);
    }

 private:
    KMonitor(const KMonitor &);
    KMonitor &operator=(const KMonitor &);
    std::string FullMetricsName(const std::string &metric_name);
    Metric* declareMetric(const std::string &name, const MetricsTags *tags);

 private:
    std::string name_;
    MetricsConfig *config_;
    MetricsManager *manager_;
    MetricsCache *metricsCache_;
    MetricsTagsManager *tags_manager_;
    std::string service_name_;
    bool service_name_setted_;
    MetricsTags *tags_;

 private:
    AUTIL_LOG_DECLARE();

 private:
    friend class KMonitorTest_TestFullMetricsName_Test;
};

typedef std::shared_ptr<KMonitor> KMonitorPtr;

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_KMONITOR_H_
