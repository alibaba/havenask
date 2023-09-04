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
#include "indexlib/index/inverted_index/truncate/SortWorkItem.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SortWorkItem);

SortWorkItem::SortWorkItem(const indexlibv2::config::TruncateProfile& truncProfile, uint32_t newDocCount,
                           TruncateAttributeReaderCreator* truncateAttrCreator,
                           const std::shared_ptr<BucketMap>& bucketMap,
                           const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema)
    : _truncateProfile(truncProfile)
    , _newDocCount(newDocCount)
    , _truncateAttrCreator(truncateAttrCreator)
    , _bucketMap(bucketMap)
    , _docInfos(nullptr)
    , _tabletSchema(tabletSchema)
{
}

void SortWorkItem::process()
{
    AUTIL_LOG(INFO, "process begin, truncate[%s]", _truncateProfile.truncateProfileName.c_str());
    _docInfos.reset(new Doc[_newDocCount]);
    for (uint32_t i = 0; i < _newDocCount; ++i) {
        _docInfos[i].docId = (docid_t)i;
    }

    DoSort(_docInfos.get(), _docInfos.get() + _newDocCount, 0);

    for (uint32_t i = 0; i < _newDocCount; ++i) {
        _bucketMap->SetSortValue(_docInfos[i].docId, i);
    }
    AUTIL_LOG(INFO, "process done, truncate[%s]", _truncateProfile.truncateProfileName.c_str());
}

void SortWorkItem::destroy() { delete this; }

void SortWorkItem::DoSort(Doc* begin, Doc* end, uint32_t sortDim)
{
    const auto& sortParams = _truncateProfile.sortParams;
    const std::string& sortField = sortParams[sortDim].GetSortField();
    auto fieldConfig = _tabletSchema->GetFieldConfig(sortField);
    assert(fieldConfig);
    switch (fieldConfig->GetFieldType()) {
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
    const auto& sortParams = _truncateProfile.sortParams;
    const std::string& sortField = sortParams[sortDim].GetSortField();
    auto attrReader = _truncateAttrCreator->Create(sortField);
    assert(attrReader);
    autil::mem_pool::UnsafePool pool;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase> ctx =
        attrReader->CreateReadContextPtr(&pool);

    uint32_t dataLen = 0;
    for (Doc* it = begin; it != end; ++it) {
        attrReader->Read(it->docId, ctx, it->GetBuffer(), sizeof(T), dataLen, it->isNull);
    }
    DocComp<T> comp(sortParams[sortDim].IsDesc());
    std::sort(begin, end, comp);
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

} // namespace indexlib::index
