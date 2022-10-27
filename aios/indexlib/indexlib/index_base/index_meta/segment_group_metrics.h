#ifndef __INDEXLIB_SEGMENT_GROUP_METRICS_H
#define __INDEXLIB_SEGMENT_GROUP_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentGroupMetrics
{
public:
    typedef autil::legacy::json::JsonMap Group;
    SegmentGroupMetrics(const Group& group)
        : mGroup(group)
    {}

    SegmentGroupMetrics(const SegmentGroupMetrics& other)
        : mGroup(other.mGroup)
    {}

public:
    // throw InconsistentStateException if failed
    template <typename MetricsType>
    MetricsType Get(const std::string& key) const
    {
        Group::const_iterator it = mGroup.find(key);
        if (it == mGroup.end())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "get [%s] from segment group metrics failed", key.c_str());
        }
        MetricsType retValue;
        autil::legacy::FromJson(retValue, it->second);
        return retValue;
    }

    template <typename MetricsType> bool Get(const std::string& key, MetricsType& value) const
    {
        auto it = mGroup.find(key);
        if (it == mGroup.end())
        {
            return false;
        }
        autil::legacy::FromJson(value, it->second);
        return true;
    }

private:
    const autil::legacy::json::JsonMap& mGroup;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentGroupMetrics);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_GROUP_METRICS_H
