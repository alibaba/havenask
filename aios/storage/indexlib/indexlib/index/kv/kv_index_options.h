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

#include "autil/cache/cache.h"
#include "indexlib/common/multi_region_rehasher.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(common, PlainFormatEncoder);

namespace indexlib { namespace index {

struct KVIndexOptions // todo codegen
{
public:
    KVIndexOptions()
        : lastSkipIncTsInSecond(INVALID_TIMESTAMP)
        , incTsInSecond(0)
        , ttl(DEFAULT_TIME_TO_LIVE)
        , buildingSegmentId(INVALID_SEGMENTID)
        , oldestRtSegmentId(INVALID_SEGMENTID)
        , fixedValueLen(-1)
        , cachePriority(autil::CacheBase::Priority::LOW)
        , hasMultiRegion(false)
    {
    }

    virtual ~KVIndexOptions() {}

public:
    void Init(const config::KVIndexConfigPtr& _kvConfig, const index_base::PartitionDataPtr& partitionData,
              int64_t latestNeedSkipIncTs);

    inline regionid_t GetRegionId() const __ALWAYS_INLINE
    {
        if (kvConfig) {
            return kvConfig->GetRegionId();
        }
        return INVALID_REGIONID;
    }

    inline uint64_t GetLookupKeyHash(uint64_t key) const __ALWAYS_INLINE
    {
        if (!hasMultiRegion) {
            return key;
        }
        return common::MultiRegionRehasher::GetRehashKey(key, kvConfig->GetRegionId());
    }

private:
    void InitPartitionState(const index_base::PartitionDataPtr& partitionData);

public:
    int64_t lastSkipIncTsInSecond;
    uint64_t incTsInSecond;
    uint64_t ttl;
    segmentid_t buildingSegmentId;
    segmentid_t oldestRtSegmentId;
    int32_t fixedValueLen;
    // bool hasBuilding; //todo codegen to const
    config::KVIndexConfigPtr kvConfig;
    std::shared_ptr<common::PlainFormatEncoder> plainFormatEncoder;
    autil::CacheBase::Priority cachePriority;
    bool hasMultiRegion; // todo codegen to const
    // bool hasOnlineSegment; //todo codegen to const

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexOptions);
}} // namespace indexlib::index
