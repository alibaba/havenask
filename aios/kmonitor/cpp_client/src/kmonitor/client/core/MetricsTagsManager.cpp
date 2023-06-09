/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-02-11 09:23
 * Author Name: yixuan
 * Author Email: yixuan@alibaba-inc.com
 */
#include "kmonitor/client/core/MetricsTagsManager.h"
#include "kmonitor/client/core/MetricsConfig.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsTagsManager);

using namespace std;

MetricsTagsManager::MetricsTagsManager(MetricsTags *tags)
    : tags_(tags)
{
    config_ = NULL;
    clearNum_ = 0;
}

MetricsTagsPtr MetricsTagsManager::GetMetricsTags(const map<string, string>& tags_map)
{
    static MetricsTagsPtr nullTagsPtr;
    if (!validateTags(tags_map)) {
        return nullTagsPtr;
    }
    MetricsTags tags(tags_map);
    if (tags_) {
        tags_->MergeTags(&tags);
    }
    uint64_t tags_hash = tags.Hashcode();

    autil::ScopedLock lock(metric_mutex_);
    std::map<uint64_t, MetricsTagsPtr>::iterator it = all_tags_.find(tags_hash);
    if (it != all_tags_.end()) {
        return it->second;
    } else {
        MetricsTagsPtr tagsPtr = MetricsTagsPtr(new MetricsTags(tags));
        auto result = all_tags_.emplace(tags_hash, tagsPtr);
        if (result.second) {
            return result.first->second;
        } else {
            AUTIL_LOG(WARN, "insert to tags_map failed");
            return MetricsTagsPtr();
        }
    }
}

void MetricsTagsManager::clearUselessTags()
{
    static const int recycle_num = 1000;
    autil::ScopedLock lock(metric_mutex_);
    size_t tags_size = all_tags_.size();
    if (tags_size < recycle_num) {
        return;
    }

    std::map<uint64_t, MetricsTagsPtr>::iterator it = all_tags_.begin();
    while (it != all_tags_.end()) {
        MetricsTagsPtr& tagsPtr = it->second;
        if (tagsPtr.use_count() == 1) {
            it = all_tags_.erase(it);
        } else {
            ++it;
        }
    }
}

bool MetricsTagsManager::validateTags(const std::map<std::string, std::string> &tags) const
{
    for (auto &t : tags) {
        if (t.first.empty() || t.second.empty())
            return false;
    }
    return true;
}

END_KMONITOR_NAMESPACE(kmonitor);
