/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-28 14:32
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/KMonitorFactory.h"

#include <map>
#include <string>

#include "autil/Log.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/KMonitorWorker.h"
#include "kmonitor/client/core/MetricsConfig.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, KMonitorFactory);

using std::map;
using std::string;
KMonitorWorker *KMonitorFactory::worker_ = NULL;

bool KMonitorFactory::Init(const string &config_content) {
    MetricsConfig config;
    if (!formatMetricsConfig(config_content, config)) {
        return false;
    }

    return Init(config);
}

bool KMonitorFactory::Init(const MetricsConfig &config) {
    if (NULL == worker_) {
        worker_ = new KMonitorWorker();
    }
    if (!worker_->Init(config)) {
        return false;
    }
    worker_->addCommonTags();
    return true;
}

bool KMonitorFactory::formatMetricsConfig(const string &config_content, MetricsConfig &config) {
    try {
        FromJsonString(config, config_content);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(
            ERROR, "parse config failed, config[%s], error_info[%s]", config_content.c_str(), e.GetMessage().c_str());
        return false;
    }

    AUTIL_LOG(INFO,
              "format config OK. service_name[%s] tenant_name[%s] sink_address[%s] global_tags[%s]",
              config.service_name().c_str(),
              config.tenant_name().c_str(),
              config.sink_address().c_str(),
              config.global_tags()->ToString().c_str());
    return true;
}

void KMonitorFactory::Start() {
    worker_->Start();
    AUTIL_LOG(INFO, "kmonitor factory started");
}

bool KMonitorFactory::IsStarted() {
    if (worker_) {
        return worker_->IsStarted();
    }
    return false;
}

void KMonitorFactory::Shutdown() {
    if (worker_ != NULL) {
        worker_->Shutdown();
        delete worker_;
        worker_ = NULL;
        AUTIL_LOG(INFO, "kmonitor factory stopped");
    }
}

void KMonitorFactory::SetServiceName(const string &name) { GetWorker()->SetServiceName(name); }

const string &KMonitorFactory::ServiceName() { return GetWorker()->ServiceName(); }

KMonitor *KMonitorFactory::GetKMonitor(const string &name, bool useMetricCache, bool useConfigTags) {
    return GetWorker()->GetKMonitor(name, useMetricCache, useConfigTags);
}

MetricsConfig *KMonitorFactory::GetConfig() { return GetWorker()->GetConfig(); }

void KMonitorFactory::ReleaseKMonitor(const string &name) { return GetWorker()->ReleaseKMonitor(name); }

void KMonitorFactory::registerBuildInMetrics(MetricsTags *tags, const string &metric_name_prefix) {
    GetWorker()->registerBuildInMetrics(tags, metric_name_prefix);
}

map<string, KMonitor *> *KMonitorFactory::GetKMonitorMap() { return GetWorker()->GetKMonitorMap(); }

KMonitorWorker *KMonitorFactory::GetWorker() {
    if (NULL == worker_) {
        worker_ = new KMonitorWorker();
    }
    return worker_;
}

END_KMONITOR_NAMESPACE(kmonitor);
