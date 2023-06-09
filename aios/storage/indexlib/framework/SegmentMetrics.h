/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/SegmentGroupMetrics.h"
#include "indexlib/util/Exception.h"

namespace indexlib::file_system {
class Directory;
class IDirectory;
} // namespace indexlib::file_system

namespace indexlib::framework {

class SegmentMetrics : public autil::legacy::Jsonizable
{
public:
    SegmentMetrics();
    virtual ~SegmentMetrics();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string ToString() const;
    void FromString(const std::string& str);
    void Store(const std::shared_ptr<file_system::Directory>& directory) const;
    void Load(const std::shared_ptr<file_system::Directory>& directory);
    Status LoadSegmentMetrics(const std::shared_ptr<file_system::IDirectory>& directory);
    Status StoreSegmentMetrics(const std::shared_ptr<file_system::IDirectory>& directory) const;

private:
    typedef autil::legacy::json::JsonMap Group;
    typedef autil::legacy::json::JsonMap GroupMap;

public:
    size_t GetDistinctTermCount(const std::string& indexName) const;
    void SetDistinctTermCount(const std::string& indexName, size_t distinctTermCount);
    void SetKeyCount(size_t keyCount);
    bool GetKeyCount(size_t& keyCount) const;
    void ListAllGroupNames(std::vector<std::string>& groupNames) const;

    template <typename MetricsType>
    void Set(const std::string& groupName, const std::string& key, const MetricsType& value);

    template <typename MetricsType>
    bool Get(const std::string& groupName, const std::string& key, MetricsType& value) const;

    // throw InconsistentStateException if groupName or key not exists
    template <typename MetricsType>
    MetricsType Get(const std::string& groupName, const std::string& key) const;

    bool IsEmpty() { return _groupMap.size() == 0; }
    bool MergeSegmentMetrics(const SegmentMetrics& otherSegmentMetrics);

    bool GetSegmentGroup(const std::string& groupName, Group& group) const;
    bool MergeSegmentGroupMetrics(const std::string groupName, const Group& otherGroup);
    std::shared_ptr<framework::SegmentGroupMetrics> GetSegmentGroupMetrics(const std::string& groupName) const;
    void SetSegmentGroupMetrics(const std::string groupName, const Group& group) { _groupMap[groupName] = group; }

private:
    static const std::string BUILT_IN_TERM_COUNT_METRICS;
    static const std::string SEGMENT_STATISTICS;
    GroupMap _groupMap;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::vector<std::shared_ptr<SegmentMetrics>> SegmentMetricsVec;

//////////////////////////////////////////////////////////////////////////////////
template <typename MetricsType>
inline void SegmentMetrics::Set(const std::string& groupName, const std::string& key, const MetricsType& value)
{
    const autil::legacy::Any& anyValue = autil::legacy::ToJson(value);

    GroupMap::iterator groupIt = _groupMap.find(groupName);
    if (groupIt != _groupMap.end()) {
        Group& group = *autil::legacy::AnyCast<Group>(&groupIt->second);
        group[key] = anyValue;
        return;
    }
    Group group({{key, anyValue}});
    _groupMap[groupName] = autil::legacy::ToJson(group);
}

template <typename MetricsType>
inline bool SegmentMetrics::Get(const std::string& groupName, const std::string& key, MetricsType& value) const
{
    GroupMap::const_iterator groupIt = _groupMap.find(groupName);
    if (groupIt == _groupMap.end()) {
        return false;
    }
    const Group& group = autil::legacy::AnyCast<Group>(groupIt->second);
    Group::const_iterator it = group.find(key);
    if (it == group.end()) {
        return false;
    }
    autil::legacy::FromJson(value, it->second);
    return true;
}

template <typename MetricsType>
inline MetricsType SegmentMetrics::Get(const std::string& groupName, const std::string& key) const
{
    MetricsType retValue;
    if (!Get(groupName, key, retValue)) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "get [%s:%s] from segment metrics failed", groupName.c_str(),
                             key.c_str());
    }
    return retValue;
}

inline bool SegmentMetrics::GetSegmentGroup(const std::string& groupName, Group& group) const
{
    GroupMap::const_iterator groupIt = _groupMap.find(groupName);
    if (groupIt == _groupMap.end()) {
        return false;
    }
    group = autil::legacy::AnyCast<Group>(groupIt->second);
    return true;
}

inline std::shared_ptr<framework::SegmentGroupMetrics>
SegmentMetrics::GetSegmentGroupMetrics(const std::string& groupName) const
{
    GroupMap::const_iterator groupIt = _groupMap.find(groupName);
    if (groupIt == _groupMap.end()) {
        return std::shared_ptr<framework::SegmentGroupMetrics>();
    }
    const Group& group = autil::legacy::AnyCast<Group>(groupIt->second);
    return std::shared_ptr<framework::SegmentGroupMetrics>(new SegmentGroupMetrics(group));
}
} // namespace indexlib::framework
