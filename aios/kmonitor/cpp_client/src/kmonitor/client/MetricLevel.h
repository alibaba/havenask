/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-08-03 09:52
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_METRICLEVEL_H_
#define KMONITOR_CLIENT_METRICLEVEL_H_

#include <map>
#include <set>
#include <string>

#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

enum MetricLevel : unsigned int {
    FATAL,
    CRITICAL,
    MAJOR,
    NORMAL,
    MINOR,
    TRIVIAL,
    METRIC_LEVEL_COUNT,
};
// kmonitor server store metrics for 20S,1m,10m,60m, sdk should align for it
struct MetricLevelConfig {
    unsigned int period[METRIC_LEVEL_COUNT] = {
        1,
        2,
        5,
        10,
        30,
        60,
    };
    MetricLevelConfig &operator=(const MetricLevelConfig &rhs) {
        std::copy(rhs.period, rhs.period + METRIC_LEVEL_COUNT, period);
        return *this;
    }
    friend std::ostream &operator<<(std::ostream &os, const MetricLevelConfig &config);
};

constexpr int64_t DEFAULT_LEVEL_TIME_MS = MetricLevelConfig{}.period[NORMAL] * 1000;

class MetricLevelManager {
public:
    MetricLevelManager();
    ~MetricLevelManager();

public:
    void AddMetricLevel(const std::string &metric_name, MetricLevel level);
    void RemoveMetricLevel(const std::string &metric_name);
    std::set<std::string> GetMetric(MetricLevel level);
    static std::set<MetricLevel> GetLevel(int second);
    static unsigned int GetLevelPeriod(MetricLevel level);
    static void SetGlobalLevelConfig(const MetricLevelConfig &config);

private:
    MetricLevelManager(const MetricLevelManager &);
    MetricLevelManager &operator=(const MetricLevelManager &);

private:
    std::map<std::string, MetricLevel> metric_map_;
    std::map<MetricLevel, std::set<std::string>> level_map_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_METRICLEVEL_H_
