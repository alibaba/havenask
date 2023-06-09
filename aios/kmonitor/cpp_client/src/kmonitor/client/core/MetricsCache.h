/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_CORE_METRICSCACHE_H_
#define KMONITOR_CLIENT_CORE_METRICSCACHE_H_

#include <string>
#include <unordered_map>
#include "autil/Log.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/metric/Metric.h"
#include "autil/Lock.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

/*
1. 不使用cache而每次KMonitor::Report(const string &name, MetricsTags *tags, double value)时，
因为每次要生成metric的fullName及mergeTags，这个耗时过长，所以改用name+tags的映射缓存metric指针。
2. 需要存下fullName-mergedTags到name-MetricsTags的映射，
原因是MetrisData里面回收时只知fullName-mergedTags
 */

class MetricsCache {
 public:
    MetricsCache();
    ~MetricsCache();

    Metric* GetCachedMetic(const std::string &name, uint64_t tags_hash);
    bool InsertCache(const std::string &name, uint64_t tags_hash, Metric* metric, const std::string &fullName, uint64_t mergedTags_hash);
    bool ClearCache(const std::string &fullName, uint64_t tags_hash, Metric* metric);

 private:
    std::map<std::pair<std::string, uint64_t>, std::pair<std::string, uint64_t> > name_map_;
    std::unordered_map<uint64_t, std::unordered_map<std::string, Metric*> > metric_cache_;
    autil::ReadWriteLock lock_;

 private:
    AUTIL_LOG_DECLARE();
};


END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_CORE_METRICSCACHE_H_
