/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <string>
#include <vector>
#include <map>
#include <sstream>
#include "autil/HashAlgorithm.h"
#include "kmonitor/client/core/MetricsTags.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;
using std::vector;
using std::map;
using std::stringstream;

MetricsTags::MetricsTags() {
    ComputeCache();
}

MetricsTags::MetricsTags(const string &k, const string &v) {
    tags_map_.emplace(k, v);
    ComputeCache();
}

MetricsTags::MetricsTags(const std::map<std::string, std::string> &tags_map)
{
    tags_map_ = tags_map;
    ComputeCache();
}

MetricsTags::~MetricsTags() {
}

void MetricsTags::ComputeCache() {
    const std::size_t prime = 19937;
    uint64_t result = 0;
    auto iter = tags_map_.begin();
    for (; iter != tags_map_.end(); ++iter) {
        result ^= prime * autil::HashAlgorithm::hashString64(iter->first.c_str()) +
                autil::HashAlgorithm::hashString64(iter->second.c_str());
    }
    cached_hash_ = result;

    std::stringstream ss;
    iter = tags_map_.begin();
    for (; iter != tags_map_.end(); ++iter) {
        ss << iter->first << "=" << iter->second << " ";
    }
    string temp = ss.str();
    if (!temp.empty()) {
        temp.erase(temp.size()-1);
    }
    cached_string_ = temp;
}

size_t MetricsTags::Size() const {
    return tags_map_.size();
}

uint64_t MetricsTags::Hashcode() const {
    return cached_hash_;
}

string MetricsTags::ToString() const {
    return cached_string_;
}

void MetricsTags::AddTag(const string &k, const string &v) {
    AddTagNoComputeCache(k, v);
    ComputeCache();
}

void MetricsTags::AddTagNoComputeCache(const string &k, const string &v) {
    tags_map_.emplace(k, v);
}

string MetricsTags::FindTag(const string &k) const {
    auto iter = tags_map_.find(k);
    if (iter != tags_map_.end()) {
        return iter->second;
    } else {
        return string();
    }
}

void MetricsTags::DelTag(const std::string& k) {
    auto iter = tags_map_.find(k);
    if (iter != tags_map_.end()) {
        tags_map_.erase(k);
        ComputeCache();
    }
}

void MetricsTags::MergeTags(MetricsTags* target_tags) const {
    auto iter = tags_map_.begin();
    for (; iter != tags_map_.end(); ++iter) {
        target_tags->AddTagNoComputeCache(iter->first, iter->second);
    }
    target_tags->ComputeCache();
}

END_KMONITOR_NAMESPACE(kmonitor);
