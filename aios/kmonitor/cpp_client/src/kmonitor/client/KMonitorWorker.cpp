/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-03-09 11:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include <map>
#include <string>
#include "autil/Log.h"
#include "kmonitor/client/common/Util.h"
#include "kmonitor/client/core/MetricsSystem.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/KMonitorWorker.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, KMonitorWorker);

using std::string;
using std::map;

KMonitorWorker::KMonitorWorker() {
    config_ = new MetricsConfig();
    system_ = new MetricsSystem();
    kmonitor_map_ = new map<string, KMonitor*>();
    started_ = false;
}

KMonitorWorker::~KMonitorWorker() {
    Shutdown();
    if (kmonitor_map_ != NULL) {
        delete kmonitor_map_;
        kmonitor_map_ = NULL;
    }
    if (config_ != NULL) {
        delete config_;
        config_ = NULL;
    }
    if (system_ != NULL) {
        delete system_;
        system_ = NULL;
    }
}

bool KMonitorWorker::Init(const string& config_content) {
    try {
        FromJsonString(*config_, config_content);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "parse config failed, config[%s], error_info[%s]",
                config_content.c_str(), e.GetMessage().c_str());
        return false;
    }
    AUTIL_LOG(INFO, "parse config content success");
    return true;
}

bool KMonitorWorker::Init(const MetricsConfig& config) {
    *config_ = config;
    AUTIL_LOG(INFO, "assgin config object success");
    return true;
}

static KMonitor* sg_buildin_kmonitor = NULL;
static MetricsTags* sg_buildin_tags = NULL;

bool KMonitorWorker::IsStarted() {
    return started_;
}

void KMonitorWorker::Start() {
    if (!config_ || !config_->inited()) {
        AUTIL_LOG(WARN, "kmonitor worker config_ is not ready");
        return;
    }
    if (started_) {
        AUTIL_LOG(WARN, "kmonitor worker has started");
        return;
    }
    addCommonTags();
    system_->Init(config_);
    started_ = true;
    //to add host,kmon_service_name for BuildInMetrics
    if (sg_buildin_kmonitor != NULL) {
        sg_buildin_kmonitor->registerBuildInMetrics(sg_buildin_tags);
    } else {
        AUTIL_LOG(INFO, "sg_buildin_kmonitor is nullptr, skip registerBuildInMetrics");
    }

    AUTIL_LOG(INFO, "kmonitor worker started");
}

void KMonitorWorker::addCommonTags() {
    // add global tag:host/kmon_service_name
    if (!config_->use_common_tag()) {
        return;
    }
    string host;
    bool ret = Util::GetHostIP(host);
    if (ret) {
        config_->AddCommonTag("host", host);
    }
    config_->AddCommonTag("hippo_cluster", string(Util::GetEnv("HIPPO_SERVICE_NAME", "default")));
    string hippoWorkDir = string(Util::GetEnv("HIPPO_WORKDIR_TAG", "default"));
    EscapeString(hippoWorkDir);
    config_->AddCommonTag("hippo_work_dir", hippoWorkDir);
    config_->AddCommonTag("hippo_app", string(Util::GetEnv("HIPPO_APP", "default")));
    config_->AddCommonTag("kmon_service_name", config_->service_name());
}

SinkPtr KMonitorWorker::GetSink(const std::string &name) const {
    return system_->GetSink(name);
}

void KMonitorWorker::EscapeString(string& input) {
    const char REPLACE_CHAR_TABLE[] = {'=', ':'};
    string oldStr = input;
    for (int i = 0;  i != sizeof(REPLACE_CHAR_TABLE); i++) {
        autil::StringUtil::replace(input, REPLACE_CHAR_TABLE[i], '-');
    }
    if (oldStr != input) {
        AUTIL_LOG(INFO, "invalid string[%s] for tag, change to [%s]",
                     oldStr.c_str(), input.c_str());
    }
}

void KMonitorWorker::Shutdown() {
    autil::ScopedLock lock(metric_mutex_);
    if (!started_) {
        AUTIL_LOG(WARN, "kmonitor worker has not started");
        return;
    }
    system_->Stop();
    map<string, KMonitor*>::iterator it = kmonitor_map_->begin();
    for (; it != kmonitor_map_->end(); it++) {
        it->second->Stop();
        AUTIL_LOG(INFO, "shutdown kmonitor [%s]", it->first.c_str());
        delete it->second;
    }
    kmonitor_map_->clear();
    started_ = false;
    // AUTIL_LOG(INFO, "kmonitor worker shutdown success");
}

void KMonitorWorker::SetServiceName(const string &name) {
    config_->set_service_name(name);
}

const string& KMonitorWorker::ServiceName() {
    return config_->service_name();
}

KMonitor* KMonitorWorker::GetKMonitor(const string &name, bool useMetricCache, bool useConfigTags) {
    autil::ScopedLock lock(metric_mutex_);
    map<string, KMonitor *>::iterator it = kmonitor_map_->find(name);
    KMonitor *kmonitor = NULL;
    if (it == kmonitor_map_->end()) {
        kmonitor = new KMonitor(name, useMetricCache);
        kmonitor->SetConfig(config_, useConfigTags);
        kmonitor_map_->insert(make_pair(name, kmonitor));
        system_->AddSource(kmonitor);
    } else {
        kmonitor = it->second;
    }
    return kmonitor;
}

void KMonitorWorker::ReleaseKMonitor(const std::string &name) {
    autil::ScopedLock lock(metric_mutex_);
    system_->DelSource(name);
    auto it = kmonitor_map_->find(name);
    if (it != kmonitor_map_->end()) {
        KMonitor *monitor = it->second;
        monitor->Stop();
        kmonitor_map_->erase(it);
        delete monitor;
    }
}

void KMonitorWorker::registerBuildInMetrics(MetricsTags *tags, const string& metric_name_prefix) {
    sg_buildin_kmonitor = GetKMonitor("buildinmetric");
    if (sg_buildin_kmonitor == NULL) {
        return;
    }
    if (sg_buildin_tags != NULL) {
        delete sg_buildin_tags;
        sg_buildin_tags = NULL;
    }

    if (tags != NULL) {
        sg_buildin_tags = new MetricsTags();
        sg_buildin_tags->MergeTags(tags);
    }

    sg_buildin_kmonitor->registerBuildInMetrics(sg_buildin_tags, NORMAL, metric_name_prefix);
}

map<string, KMonitor*>* KMonitorWorker::GetKMonitorMap() {
    return kmonitor_map_;
}

END_KMONITOR_NAMESPACE(kmonitor);
