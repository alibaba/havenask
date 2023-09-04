/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/core/MetricsCache.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsCache);

using namespace std;

MetricsCache::MetricsCache() {}

MetricsCache::~MetricsCache() {}

Metric *MetricsCache::GetCachedMetic(const std::string &name, uint64_t tags_hash) {
    Metric *metric = NULL;
    autil::ScopedReadLock rlock(lock_);
    auto iter = metric_cache_.find(tags_hash);
    if (iter != metric_cache_.end()) {
        auto &metricMap = iter->second;
        auto metricIter = metricMap.find(name);
        if (metricIter != metricMap.end()) {
            metric = metricIter->second;
        }
    }

    return metric;
}

bool MetricsCache::InsertCache(const std::string &name,
                               uint64_t tags_hash,
                               Metric *metric,
                               const std::string &fullName,
                               uint64_t mergedTags_hash) {
    if (metric == NULL) {
        return false;
    }

    pair<string, uint64_t> name_tag = make_pair(name, tags_hash);
    pair<string, uint64_t> fullName_tag = make_pair(fullName, mergedTags_hash);
    autil::ScopedWriteLock wlock(lock_);
    map<pair<string, uint64_t>, pair<string, uint64_t>>::iterator nameIter = name_map_.find(fullName_tag);
    if (nameIter != name_map_.end()) {
        AUTIL_LOG(DEBUG,
                  "metric[%s] tags_hash[%lu] is existed for fullName[%s] mergedTags[%lu]",
                  name.c_str(),
                  tags_hash,
                  fullName.c_str(),
                  mergedTags_hash);
        return false;
    }

    auto iter = metric_cache_.find(tags_hash);
    if (iter != metric_cache_.end()) {
        auto &metricMap = iter->second;
        auto metricIter = metricMap.find(name);
        if (metricIter != metricMap.end()) {
            AUTIL_LOG(DEBUG, "metric[%s] tags_hash[%lu] is existed", name.c_str(), tags_hash);
            return false;
        } else {
            metricMap.insert(make_pair(name, metric));
        }
    } else {
        unordered_map<string, Metric *> metricMap;
        metricMap.insert(make_pair(name, metric));
        metric_cache_.insert(make_pair(tags_hash, metricMap));
    }

    name_map_.insert(make_pair(fullName_tag, name_tag));

    AUTIL_LOG(DEBUG,
              "success to insert cache for <name[%s] tags_hash[%lu]> to <full_name[%s] mergedTags_hash[%lu]>",
              name.c_str(),
              tags_hash,
              fullName.c_str(),
              mergedTags_hash);

    return true;
}

bool MetricsCache::ClearCache(const std::string &fullName, uint64_t mergedTags_hash, Metric *metric) {
    pair<string, uint64_t> fullName_tag = make_pair(fullName, mergedTags_hash);
    autil::ScopedWriteLock wlock(lock_);
    map<pair<string, uint64_t>, pair<string, uint64_t>>::iterator nameIter = name_map_.find(fullName_tag);
    if (nameIter == name_map_.end()) {
        AUTIL_LOG(DEBUG, "can't find metric[%s] for mergedTags_hash[%lu]", fullName.c_str(), mergedTags_hash);
        return false;
    }

    pair<string, uint64_t> &name_tag = nameIter->second;
    string &name = name_tag.first;
    uint64_t tags_hash = name_tag.second;

    auto meticCacheIter = metric_cache_.find(tags_hash);
    if (meticCacheIter == metric_cache_.end()) {
        AUTIL_LOG(DEBUG, "can't find metric_cache for metric_name[%s] tags_hash[%lu]", name.c_str(), tags_hash);
        return false;
    }

    auto &metricMap = meticCacheIter->second;
    auto metricIter = metricMap.find(name);
    if (metricIter == metricMap.end()) {
        AUTIL_LOG(DEBUG, "can't find metric for metric_name[%s] tags_hash[%lu]", name.c_str(), tags_hash);
        return false;
    }

    if (metric != metricIter->second) {
        AUTIL_LOG(DEBUG,
                  "metric[%p] not match [%p] for metric_name[%s] tags_hash[%lu]",
                  metric,
                  metricIter->second,
                  name.c_str(),
                  tags_hash);
        return false;
    }

    metricMap.erase(name);

    if (metricMap.empty()) {
        metric_cache_.erase(tags_hash);
    }

    name_map_.erase(nameIter);

    AUTIL_LOG(DEBUG,
              "success to clear cache for <name[%s] tags_hash[%lu]> to <full_name[%s] mergedTags_hash[%lu]>",
              name.c_str(),
              tags_hash,
              fullName.c_str(),
              mergedTags_hash);

    return true;
}

END_KMONITOR_NAMESPACE(kmonitor);
