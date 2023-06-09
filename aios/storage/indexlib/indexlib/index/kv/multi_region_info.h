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
#ifndef __INDEXLIB_MULTI_REGION_INFO_H
#define __INDEXLIB_MULTI_REGION_INFO_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SingleRegionInfo : public autil::legacy::Jsonizable
{
public:
    SingleRegionInfo(regionid_t regionId = DEFAULT_REGIONID) : mRegionId(regionId), mItemCount(0) {}

    ~SingleRegionInfo() {}

    void AddItemCount(uint64_t count) { mItemCount += count; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("region_id", mRegionId, mRegionId);
        json.Jsonize("item_count", mItemCount, mItemCount);
    }

private:
    regionid_t mRegionId;
    uint64_t mItemCount;
};

DEFINE_SHARED_PTR(SingleRegionInfo);

////////////////////////////////////////////////////////////

class MultiRegionInfo : public autil::legacy::Jsonizable
{
public:
    MultiRegionInfo() {}
    MultiRegionInfo(uint32_t regionCount) { Init(regionCount); }

    ~MultiRegionInfo() {}

public:
    void Init(uint32_t regionCount)
    {
        mRegionInfoVec.reserve(regionCount);
        for (uint32_t i = 0; i < regionCount; i++) {
            SingleRegionInfoPtr regionInfo(new SingleRegionInfo(i));
            mRegionInfoVec.push_back(regionInfo);
        }
    }

    const SingleRegionInfoPtr& Get(uint32_t idx) const
    {
        assert(idx < mRegionInfoVec.size());
        return mRegionInfoVec[idx];
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("region_infos", mRegionInfoVec);
    }

    void Store(const file_system::DirectoryPtr& dir)
    {
        std::string content = autil::legacy::ToJsonString(*this);
        dir->Store("region_info", content, file_system::WriterOption());
    }

private:
    std::vector<SingleRegionInfoPtr> mRegionInfoVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionInfo);
}} // namespace indexlib::index

#endif //__INDEXLIB_MULTI_REGION_INFO_H
