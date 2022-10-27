#ifndef __INDEXLIB_SEGMENT_METRICS_UTIL_H
#define __INDEXLIB_SEGMENT_METRICS_UTIL_H

#include<autil/StringUtil.h>

IE_NAMESPACE_BEGIN(index_base);

class SegmentMetricsUtil
{
public:
    static std::string GetColumnGroupName(uint32_t columnIdx)
    {
        return std::string("segment_stat_metrics_@_") +
            autil::StringUtil::toString<uint32_t>(columnIdx);
    }
};

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_METRICS_UTIL_H
