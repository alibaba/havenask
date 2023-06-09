/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_CORE_METRICSTAGS_H_
#define KMONITOR_CLIENT_CORE_METRICSTAGS_H_

#include <string>
#include <map>
#include <vector>
#include <mutex>
#include "kmonitor/client/common/Common.h"


BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsTags {
 public:
    MetricsTags();
    MetricsTags(const std::string &k, const std::string &v);
    MetricsTags(const std::map<std::string, std::string> &tags_map);
    ~MetricsTags();

    void AddTag(const std::string &k, const std::string &v);
    std::string FindTag(const std::string &k) const;
    void DelTag(const std::string& k);
    void MergeTags(MetricsTags* tags) const;
    size_t Size() const;
    uint64_t Hashcode() const;
    std::string ToString() const;
    const std::map<std::string, std::string>& GetTagsMap() const {
        return tags_map_;
    }

 private:
    void AddTagNoComputeCache(const std::string &k, const std::string &v);
    void ComputeCache();
 private:
    mutable uint64_t cached_hash_ = 0;
    mutable std::string cached_string_;
    std::map<std::string, std::string> tags_map_;

 private:
    friend class MetricsTagsTest_TestAddTag_Test;
    friend class MetricsTagsTest_TestMergeTags_Test;
};

inline bool operator<(const MetricsTags &ls, const MetricsTags &rs) {
    return false;
}

typedef std::shared_ptr<const MetricsTags> MetricsTagsPtr;

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_CORE_METRICSTAGS_H_
