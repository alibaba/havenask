/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-08-03 10:12
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include <set>
#include <map>
#include <string>
#include "kmonitor/client/MetricLevel.h"
#include "autil/StringUtil.h"

using namespace std;

BEGIN_KMONITOR_NAMESPACE(kmonitor);

AUTIL_LOG_SETUP(kmonitor, MetricLevelManager);

static MetricLevelConfig globalLevelConfig;

MetricLevelManager::MetricLevelManager() {
}

MetricLevelManager::~MetricLevelManager() {
}

void MetricLevelManager::AddMetricLevel(const string& metric_name, MetricLevel level) {
    metric_map_[metric_name] = level;
    map<MetricLevel, set<string> >::iterator iter = level_map_.find(level);
    if (iter != level_map_.end()) {
        iter->second.insert(metric_name);
    } else {
        set<string> metric_set;
        metric_set.insert(metric_name);
        level_map_.insert(make_pair(level, metric_set));
    }
}

void MetricLevelManager::RemoveMetricLevel(const string& metric_name) {
    map<string, MetricLevel>::iterator iter = metric_map_.find(metric_name);
    if (iter != metric_map_.end()) {
        MetricLevel level = iter->second;
        metric_map_.erase(iter);

        map<MetricLevel, set<string> >::iterator it = level_map_.find(level);
        if (it != level_map_.end()) {
            it->second.erase(metric_name);
        }
    }
}

set<string> MetricLevelManager::GetMetric(MetricLevel level) {
    map<MetricLevel, set<string> >::iterator iter = level_map_.find(level);
    if (iter != level_map_.end()) {
        return iter->second;
    } else {
        set<string> empty;
        return empty;
    }
}

set<MetricLevel> MetricLevelManager::GetLevel(int second) {
    set<MetricLevel> levels;
#define CODE_GEN_IMPL(level)                                    \
    if ((second % globalLevelConfig.period[level]) == 0) {      \
        levels.insert(level);                                   \
    }
    CODE_GEN_IMPL(FATAL);
    CODE_GEN_IMPL(CRITICAL);
    CODE_GEN_IMPL(MAJOR);
    CODE_GEN_IMPL(NORMAL);
    CODE_GEN_IMPL(MINOR);
    CODE_GEN_IMPL(TRIVIAL);
#undef CODE_GEN_IMPL
    return levels;
}

unsigned int MetricLevelManager::GetLevelPeriod(MetricLevel level) {
    return globalLevelConfig.period[level];
}

void MetricLevelManager::SetGlobalLevelConfig(const MetricLevelConfig &config) {
    globalLevelConfig = config;
    AUTIL_LOG(INFO, "set global level config [%s]",
                 autil::StringUtil::toString(globalLevelConfig).c_str());
}

std::ostream &operator<<(std::ostream &os, const MetricLevelConfig &config) {
    os << "period: ";
    autil::StringUtil::toStream(os, config.period, config.period + METRIC_LEVEL_COUNT);
    return os;
}

END_KMONITOR_NAMESPACE(kmonitor);

