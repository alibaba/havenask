#ifndef __INDEXLIB_ONLINE_JOIN_POLICY_H
#define __INDEXLIB_ONLINE_JOIN_POLICY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(common, IndexLocator);

IE_NAMESPACE_BEGIN(index);

class OnlineJoinPolicy
{
public:
    OnlineJoinPolicy(const index_base::Version& incVersion,
                     TableType tableType)
        : mIncVersion(incVersion)
        , mTableType(tableType)
    {}
    ~OnlineJoinPolicy() {}
public:
    // reclaim rt data which ts < reclaimTimestamp
    int64_t GetReclaimRtTimestamp() const;

    // return realtime build seek timestamp based on version
    int64_t GetRtSeekTimestampFromIncVersion() const;

    int64_t GetRtSeekTimestamp(
        const common::IndexLocator& indexLocator,
        bool& fromInc) const;

    static int64_t GetRtSeekTimestamp(
        int64_t incVersionTs, int64_t currentRtTs, bool& fromInc);

    // doc.ts < rtFilterTs, will be dropped in realtime build
    int64_t GetRtFilterTimestamp() const;

private:
    const index_base::Version& mIncVersion;
    TableType mTableType;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineJoinPolicy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ONLINE_JOIN_POLICY_H
