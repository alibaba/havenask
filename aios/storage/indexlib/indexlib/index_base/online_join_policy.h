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
#ifndef __INDEXLIB_ONLINE_JOIN_POLICY_H
#define __INDEXLIB_ONLINE_JOIN_POLICY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(document, IndexLocator);

namespace indexlib { namespace index_base {

class OnlineJoinPolicy
{
public:
    OnlineJoinPolicy(const index_base::Version& incVersion, TableType tableType, document::SrcSignature src)
        : mIncVersion(incVersion)
        , mTableType(tableType)
        , mSrcSignature(src)
    {
    }
    ~OnlineJoinPolicy() {}

public:
    // reclaim rt data which ts < reclaimTimestamp
    int64_t GetReclaimRtTimestamp() const;

    // return realtime build seek timestamp based on version
    int64_t GetRtSeekTimestampFromIncVersion() const;

    int64_t GetRtSeekTimestamp(const document::IndexLocator& indexLocator, bool& fromInc) const;

    static int64_t GetRtSeekTimestamp(int64_t incVersionTs, int64_t currentRtTs, bool& fromInc);

    // doc.ts < rtFilterTs, will be dropped in realtime build
    int64_t GetRtFilterTimestamp() const;

private:
    const index_base::Version& mIncVersion;
    TableType mTableType;
    document::SrcSignature mSrcSignature;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineJoinPolicy);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_ONLINE_JOIN_POLICY_H
