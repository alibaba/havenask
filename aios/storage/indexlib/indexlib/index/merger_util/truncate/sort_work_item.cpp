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
#include "indexlib/index/merger_util/truncate/sort_work_item.h"

#include "indexlib/index/merger_util/truncate/evaluator_creator.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib::index::legacy {
IE_LOG_SETUP(index, SortWorkItem);

SortWorkItem::SortWorkItem(const config::TruncateProfile& truncProfile, uint32_t newDocCount,
                           TruncateAttributeReaderCreator* truncateAttrCreator, const BucketMapPtr& bucketMap)
{
    mTruncProfile = truncProfile;
    mNewDocCount = newDocCount;
    mTruncateAttrCreator = truncateAttrCreator;
    mBucketMap = bucketMap;
    mDocInfos = NULL;
}

SortWorkItem::~SortWorkItem() { Destroy(); }

void SortWorkItem::Process()
{
    IE_LOG(INFO, "process begin");
    mDocInfos = new Doc[mNewDocCount];
    for (uint32_t i = 0; i < mNewDocCount; ++i) {
        mDocInfos[i].mDocId = (docid_t)i;
    }
    IE_LOG(INFO, "sort begin");
    DoSort(mDocInfos, mDocInfos + mNewDocCount, 0);
    IE_LOG(INFO, "sort done");
    for (uint32_t i = 0; i < mNewDocCount; ++i) {
        mBucketMap->SetSortValue(mDocInfos[i].mDocId, i);
    }
    IE_LOG(INFO, "process done");
}

void SortWorkItem::Destroy()
{
    if (mDocInfos) {
        delete[] mDocInfos;
        mDocInfos = NULL;
    }
    mBucketMap.reset();
}

void SortWorkItem::DoSort(Doc* begin, Doc* end, uint32_t sortDim)
{
    const config::SortParams& sortParams = mTruncProfile.mSortParams;
    const std::string& sortField = sortParams[sortDim].GetSortField();
    const AttributeSchemaPtr& attrSchema = mTruncateAttrCreator->GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(sortField);
    assert(attrConfig);
    switch (attrConfig->GetFieldType()) {
    case ft_int8:
        return SortTemplate<int8_t>(begin, end, sortDim);
    case ft_uint8:
        return SortTemplate<uint8_t>(begin, end, sortDim);
    case ft_int16:
        return SortTemplate<int16_t>(begin, end, sortDim);
    case ft_uint16:
        return SortTemplate<uint16_t>(begin, end, sortDim);
    case ft_int32:
        return SortTemplate<int32_t>(begin, end, sortDim);
    case ft_uint32:
        return SortTemplate<uint32_t>(begin, end, sortDim);
    case ft_int64:
        return SortTemplate<int64_t>(begin, end, sortDim);
    case ft_hash_64:
    case ft_uint64:
        return SortTemplate<uint64_t>(begin, end, sortDim);
    case ft_float:
        return SortTemplate<float>(begin, end, sortDim);
    case ft_fp8:
        return SortTemplate<int8_t>(begin, end, sortDim);
    case ft_fp16:
        return SortTemplate<int16_t>(begin, end, sortDim);
    case ft_double:
        return SortTemplate<double>(begin, end, sortDim);
    case ft_date:
        return SortTemplate<uint32_t>(begin, end, sortDim);
    case ft_time:
        return SortTemplate<uint32_t>(begin, end, sortDim);
    case ft_timestamp:
        return SortTemplate<uint64_t>(begin, end, sortDim);
    default:
        assert(false);
    }
}

template <typename T>
void SortWorkItem::SortTemplate(Doc* begin, Doc* end, uint32_t sortDim)
{
    const config::SortParams& sortParams = mTruncProfile.mSortParams;
    const std::string& sortField = sortParams[sortDim].GetSortField();
    auto attrReader = mTruncateAttrCreator->Create(sortField);
    assert(attrReader);
    autil::mem_pool::UnsafePool pool;
    AttributeSegmentReader::ReadContextBasePtr ctx = attrReader->CreateReadContextPtr(&pool);

    for (Doc* it = begin; it != end; ++it) {
        attrReader->Read(it->mDocId, ctx, it->GetBuffer(), sizeof(T), it->mIsNull);
    }
    DocComp<T> comp(sortParams[sortDim].IsDesc());
    sort(begin, end, comp);
    if (sortDim == sortParams.size() - 1) {
        return;
    }
    for (Doc* it = begin; it != end;) {
        Doc* it2 = it + 1;
        for (; it2 != end; ++it2) {
            if (it->GetRef<T>() != it2->GetRef<T>()) {
                break;
            }
        }
        if (std::distance(it, it2) > 1) {
            DoSort(it, it2, sortDim + 1);
        }
        it = it2;
    }
}

int64_t SortWorkItem::EstimateMemoryUse(uint32_t totalDocCount)
{
    int64_t size = sizeof(Doc) * totalDocCount;
    size += sizeof(int64_t) * totalDocCount; // for sort attribute reader
    return size;
}
} // namespace indexlib::index::legacy
