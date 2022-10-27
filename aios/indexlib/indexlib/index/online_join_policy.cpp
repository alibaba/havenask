#include "indexlib/index/online_join_policy.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/document/locator.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/index_define.h"

using namespace std;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnlineJoinPolicy);

int64_t OnlineJoinPolicy::GetReclaimRtTimestamp() const
{
    // for oldVersion, reclaim doc.ts <= version.ts
    // because offline cannot guarantee version.ts >= version.stopTs
    if (mIncVersion.GetFormatVersion() < 2u)
    {
        return mIncVersion.GetTimestamp() + 1;
    }
    // for kv/kkv, reclaim segment.ts <= version.ts
    if (mTableType == tt_kv || mTableType == tt_kkv)
    {
        return mIncVersion.GetTimestamp() + 1;
    }
    return mIncVersion.GetTimestamp();
}

int64_t OnlineJoinPolicy::GetRtSeekTimestampFromIncVersion() const
{
    if (mIncVersion.GetVersionId() == INVALID_VERSION)
    {
        return INVALID_TIMESTAMP;
    }
    int64_t incVersionTs = mIncVersion.GetTimestamp();
    if (incVersionTs != INVALID_TIMESTAMP
        && (mTableType == tt_kv || mTableType == tt_kkv))
    {
        return SecondToMicrosecond(MicrosecondToSecond(incVersionTs));
    }
    return incVersionTs;
}

int64_t OnlineJoinPolicy::GetRtSeekTimestamp(
    const common::IndexLocator& indexLocator, bool& fromInc) const
{
    assert(indexLocator != common::INVALID_INDEX_LOCATOR);
    int64_t incSeekTs = GetRtSeekTimestampFromIncVersion();
    return GetRtSeekTimestamp(incSeekTs, indexLocator.getOffset(), fromInc);
}

int64_t OnlineJoinPolicy::GetRtSeekTimestamp(
    int64_t incVersionTs, int64_t currentRtTs, bool& fromInc)
{
    if (incVersionTs > currentRtTs)
    {
        fromInc = true;
        return incVersionTs;
    }
    fromInc = false;
    return currentRtTs;
}

int64_t OnlineJoinPolicy::GetRtFilterTimestamp() const
{
    int64_t rtSeekTs = GetRtSeekTimestampFromIncVersion();
    if (rtSeekTs == INVALID_TIMESTAMP)
    {
        return INVALID_TIMESTAMP;
    }
    if (mIncVersion.GetFormatVersion() < 2u)
    {
        if (mTableType == tt_kv || mTableType == tt_kkv)
        {
            return rtSeekTs;
        }
        // for index table, drop doc.ts <= version.ts
        return rtSeekTs + 1; 
    }
    else
    {
        return rtSeekTs;
    }
}


IE_NAMESPACE_END(index);

