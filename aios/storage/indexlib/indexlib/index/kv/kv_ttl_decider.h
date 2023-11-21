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

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KVTTLDecider
{
public:
    KVTTLDecider();
    ~KVTTLDecider();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema);

    inline bool IsExpiredItem(regionid_t regionId, uint64_t itemTsInSec, uint64_t currentTsInSec) const
    {
        if (regionId < 0 || regionId > (regionid_t)mTTLVec.size()) {
            IE_LOG(ERROR, "regionId [%d] out of range [%lu]", regionId, mTTLVec.size());
            return false;
        }
        if (!mTTLVec[regionId].first) {
            return false;
        }
        uint64_t itemDeadLine = itemTsInSec + mTTLVec[regionId].second;
        return itemDeadLine < currentTsInSec;
    }

private:
    typedef std::pair<bool, uint64_t> InnerInfo;
    std::vector<InnerInfo> mTTLVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVTTLDecider);
}} // namespace indexlib::index
