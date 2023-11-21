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
#include "indexlib/index_base/online_join_policy.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/document/locator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, OnlineJoinPolicy);

int64_t OnlineJoinPolicy::GetReclaimRtTimestamp() const
{
    // for oldVersion, reclaim doc.ts <= version.ts
    // because offline cannot guarantee version.ts >= version.stopTs
    if (mIncVersion.GetFormatVersion() < 2u) {
        return mIncVersion.GetTimestamp() + 1;
    }
    // for kv/kkv, reclaim segment.ts <= version.ts
    if (mTableType == tt_kv || mTableType == tt_kkv) {
        return mIncVersion.GetTimestamp() + 1;
    }
    return mIncVersion.GetTimestamp();
}

int64_t OnlineJoinPolicy::GetRtSeekTimestampFromIncVersion() const
{
    if (mIncVersion.GetVersionId() == INVALID_VERSIONID) {
        return INVALID_TIMESTAMP;
    }

    int64_t incVersionTs = mIncVersion.GetTimestamp();
    if (incVersionTs != INVALID_TIMESTAMP && (mTableType == tt_kv || mTableType == tt_kkv)) {
        incVersionTs = autil::TimeUtility::sec2us(autil::TimeUtility::us2sec(incVersionTs));
    }
    document::IndexLocator locator;
    bool hasLocator = locator.fromString(mIncVersion.GetLocator().GetLocator());
    uint64_t src = 0;
    if (mSrcSignature.Get(&src)) {
        if (!hasLocator || locator.getSrc() != src) {
            incVersionTs = INVALID_TIMESTAMP;
        }
    }
    return incVersionTs;
}

int64_t OnlineJoinPolicy::GetRtSeekTimestamp(const document::IndexLocator& indexLocator, bool& fromInc) const
{
    assert(indexLocator != document::INVALID_INDEX_LOCATOR);
    int64_t incSeekTs = GetRtSeekTimestampFromIncVersion();
    return GetRtSeekTimestamp(incSeekTs, indexLocator.getOffset(), fromInc);
}

int64_t OnlineJoinPolicy::GetRtSeekTimestamp(int64_t incVersionTs, int64_t currentRtTs, bool& fromInc)
{
    if (incVersionTs > currentRtTs) {
        fromInc = true;
        return incVersionTs;
    }
    fromInc = false;
    return currentRtTs;
}

int64_t OnlineJoinPolicy::GetRtFilterTimestamp() const
{
    int64_t rtSeekTs = GetRtSeekTimestampFromIncVersion();
    if (rtSeekTs == INVALID_TIMESTAMP) {
        return INVALID_TIMESTAMP;
    }
    if (mIncVersion.GetFormatVersion() < 2u) {
        if (mTableType == tt_kv || mTableType == tt_kkv) {
            return rtSeekTs;
        }
        // for index table, drop doc.ts <= version.ts
        return rtSeekTs + 1;
    } else {
        return rtSeekTs;
    }
}
}} // namespace indexlib::index_base
