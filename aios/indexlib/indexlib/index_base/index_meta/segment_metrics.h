#ifndef __INDEXLIB_SEGMENT_METRICS_H
#define __INDEXLIB_SEGMENT_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_base/index_meta/segment_group_metrics.h"
#include <autil/legacy/jsonizable.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

class SegmentMetrics: public autil::legacy::Jsonizable
{
public:
    SegmentMetrics();
    ~SegmentMetrics();
    
public:
    size_t GetDistinctTermCount(const std::string &indexName) const;
    void SetDistinctTermCount(const std::string &indexName, size_t distinctTermCount);

private:
    typedef autil::legacy::json::JsonMap Group;
    typedef autil::legacy::json::JsonMap GroupMap;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    template <typename MetricsType>
    void Set(const std::string& groupName,
             const std::string& key, const MetricsType& value);

    template <typename MetricsType>
    bool Get(const std::string& groupName,
             const std::string& key, MetricsType& value) const;

    // throw InconsistentStateException if groupName or key not exists
    template <typename MetricsType>
    MetricsType Get(const std::string& groupName, const std::string& key) const;

    index_base::SegmentGroupMetricsPtr GetSegmentGroupMetrics(const std::string& groupName) const;
    void SetSegmentGroupMetrics(const std::string groupName, const Group& group)
    {
        mGroupMap[groupName] = group;
    }

    index_base::SegmentGroupMetricsPtr GetSegmentCustomizeGroupMetrics() const
    {
        return GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    }
    void SetSegmentCustomizeGroupMetrics(const Group& group)
    {
        SetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP, group);
    }

    std::string ToString() const;
    void FromString(const std::string& str);

    void Store(const file_system::DirectoryPtr& directory) const;
    void Load(const file_system::DirectoryPtr& directory);
    void Store(const std::string& path) const;
    void Load(const std::string& path);

private:
    static const std::string BUILT_IN_TERM_COUNT_METRICS;
    GroupMap mGroupMap;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentMetrics);
typedef std::vector<SegmentMetricsPtr> SegmentMetricsVec;

//////////////////////////////////////////////////////////////////////////////////
template <typename MetricsType>
inline void SegmentMetrics::Set(const std::string& groupName,
                                const std::string& key, const MetricsType& value)
{
    const autil::legacy::Any& anyValue = autil::legacy::ToJson(value);

    GroupMap::iterator groupIt = mGroupMap.find(groupName);
    if (groupIt != mGroupMap.end())
    {
        Group& group = *autil::legacy::AnyCast<Group>(&groupIt->second);
        group[key] = anyValue;
        return;
    }
    Group group({{key, anyValue}});
    mGroupMap[groupName] = autil::legacy::ToJson(group);
}

template <typename MetricsType>
inline bool SegmentMetrics::Get(const std::string& groupName,
                                const std::string& key, MetricsType& value) const
{
    GroupMap::const_iterator groupIt = mGroupMap.find(groupName);
    if (groupIt == mGroupMap.end())
    {
        return false;
    }
    const Group& group = autil::legacy::AnyCast<Group>(groupIt->second);
    Group::const_iterator it = group.find(key);
    if (it == group.end())
    {
        return false;
    }
    autil::legacy::FromJson(value, it->second);
    return true;
}

template <typename MetricsType>
inline MetricsType SegmentMetrics::Get(const std::string& groupName,
                                       const std::string& key) const
{
    MetricsType retValue;
    if (!Get(groupName, key, retValue))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "get [%s:%s] from segment metrics failed",
                             groupName.c_str(), key.c_str());
    }
    return retValue;
}

inline index_base::SegmentGroupMetricsPtr SegmentMetrics::GetSegmentGroupMetrics(
        const std::string& groupName) const
{
    GroupMap::const_iterator groupIt = mGroupMap.find(groupName);
    if (groupIt == mGroupMap.end())
    {
        return index_base::SegmentGroupMetricsPtr();
    }
    const Group& group = autil::legacy::AnyCast<Group>(groupIt->second);
    return index_base::SegmentGroupMetricsPtr(new SegmentGroupMetrics(group));
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_METRICS_H
