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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/IndexerOrganizerMeta.h"
#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class Segment;
class TabletData;
} // namespace indexlibv2::framework

namespace indexlibv2::index {
class IDiskIndexer;
class IMemIndexer;
class AttributeConfig;
} // namespace indexlibv2::index

namespace indexlib::index {

template <typename DiskIndexerType, typename MemIndexerType>
struct SingleAttributeBuildInfoHolder {
    SingleAttributeBuildInfoHolder()
    {
        // both indexConfig and attributeConfig are needed in order to support virtual attribute.
        indexConfig = nullptr;
        attributeConfig = nullptr;
        attributeConvertor = nullptr;
        buildingIndexer = nullptr;
    }
    // IndexConfig here might be a VirtualAttributeConfig or an AttributeConfig.
    // VirtualAttributeConfig and AttributeConfig return different index types. So when we need to call GetIndexType(),
    // we should use indexConfig->GetIndexType().
    std::shared_ptr<indexlibv2::config::IIndexConfig> indexConfig;
    std::shared_ptr<indexlibv2::index::AttributeConfig> attributeConfig;
    std::shared_ptr<indexlibv2::index::AttributeConvertor> attributeConvertor;
    std::map<std::pair<docid_t, uint32_t>, std::shared_ptr<DiskIndexerType>> diskIndexers;
    std::map<std::pair<docid_t, uint64_t>, indexlibv2::framework::Segment*> diskSegments;
    std::map<std::pair<docid_t, uint32_t>, std::shared_ptr<MemIndexerType>> dumpingIndexers;
    std::shared_ptr<MemIndexerType> buildingIndexer;
    std::string nullValue;
};

class AttributeIndexerOrganizerUtil
{
public:
    template <typename DiskIndexerType, typename MemIndexerType>
    static bool UpdateField(docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
                            SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder,
                            const autil::StringView& value, bool isNull);
    template <typename DiskIndexerType, typename MemIndexerType>
    static bool UpdateField(docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
                            SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder,
                            const autil::StringView& value, bool isNull, const uint64_t* hashKey);

    template <typename DiskIndexerType, typename MemIndexerType>
    static Status CreateSingleAttributeBuildInfoHolder(
        const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
        const std::shared_ptr<indexlibv2::index::AttributeConfig>& attributeConfig,
        SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder);

private:
    template <typename DiskIndexerType, typename MemIndexerType>
    static bool
    UpdateFieldInDiskIndexer(SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder,
                             const docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull,
                             const uint64_t* hashKey);
    template <typename DiskIndexerType, typename MemIndexerType>
    static bool
    UpdateFieldInDumpingIndexer(SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder,
                                const docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull,
                                const uint64_t* hashKey);

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////

template <typename DiskIndexerType, typename MemIndexerType>
Status AttributeIndexerOrganizerUtil::CreateSingleAttributeBuildInfoHolder(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlibv2::index::AttributeConfig>& attributeConfig,
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder)
{
    buildInfoHolder->indexConfig = indexConfig;
    buildInfoHolder->attributeConfig = attributeConfig;
    const auto& fieldConfig = attributeConfig->GetFieldConfig();

    auto attributeConvertor = std::shared_ptr<indexlibv2::index::AttributeConvertor>(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attributeConfig));
    buildInfoHolder->attributeConvertor = attributeConvertor;
    if (fieldConfig->IsMultiValue() || fieldConfig->GetFieldType() == ft_string) {
        auto [st, curNullValue] = attributeConvertor->EncodeNullValue();
        RETURN_IF_STATUS_ERROR(st, "Encode null value failed, fieldId[%d].", attributeConfig->GetFieldId());
        buildInfoHolder->nullValue = curNullValue;
    }
    return Status::OK();
}

template <typename DiskIndexerType, typename MemIndexerType>
bool AttributeIndexerOrganizerUtil::UpdateField(
    docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder, const autil::StringView& value,
    bool isNull)
{
    const indexlibv2::index::AttributeConfig* attributeConfig = buildInfoHolder->attributeConfig.get();
    assert(attributeConfig != nullptr);
    if (!attributeConfig->IsAttributeUpdatable()) {
        return false;
    }
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig = attributeConfig->GetFieldConfig();
    assert(fieldConfig != nullptr);

    if (isNull && (fieldConfig->IsMultiValue() || fieldConfig->GetFieldType() == ft_string)) {
        indexlibv2::index::AttrValueMeta meta =
            buildInfoHolder->attributeConvertor->Decode(autil::StringView(buildInfoHolder->nullValue));
        return UpdateField<DiskIndexerType, MemIndexerType>(docId, indexerOrganizerMeta, buildInfoHolder, meta.data,
                                                            isNull, &meta.hashKey);
    }
    static const uint64_t* invalidHashKey = nullptr;
    return UpdateField<DiskIndexerType, MemIndexerType>(docId, indexerOrganizerMeta, buildInfoHolder, value, isNull,
                                                        invalidHashKey);
}

template <typename DiskIndexerType, typename MemIndexerType>
bool AttributeIndexerOrganizerUtil::UpdateField(
    docid_t docId, const IndexerOrganizerMeta& indexerOrganizerMeta,
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder, const autil::StringView& value,
    bool isNull, const uint64_t* hashKey)
{
    const indexlibv2::index::AttributeConfig* attributeConfig = buildInfoHolder->attributeConfig.get();
    const fieldid_t fieldId = attributeConfig->GetFieldId();
    if (docId < indexerOrganizerMeta.dumpingBaseDocId) {
        return UpdateFieldInDiskIndexer<DiskIndexerType, MemIndexerType>(buildInfoHolder, docId, fieldId, value, isNull,
                                                                         hashKey);
    }
    if (docId < indexerOrganizerMeta.buildingBaseDocId) {
        return UpdateFieldInDumpingIndexer<DiskIndexerType, MemIndexerType>(buildInfoHolder, docId, fieldId, value,
                                                                            isNull, hashKey);
    }
    if (buildInfoHolder->buildingIndexer != nullptr) {
        return std::dynamic_pointer_cast<MemIndexerType>(buildInfoHolder->buildingIndexer)
            ->UpdateField(docId - indexerOrganizerMeta.buildingBaseDocId, value, isNull, hashKey);
    }
    AUTIL_INTERVAL_LOG2(10, WARN,
                        "Updating field [%d] with docId [%d] value [%s] to default value indexer is not supported",
                        attributeConfig->GetFieldId(), docId, value.to_string().c_str());
    return true;
}

template <typename DiskIndexerType, typename MemIndexerType>
bool AttributeIndexerOrganizerUtil::UpdateFieldInDiskIndexer(
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder, const docid_t docId,
    fieldid_t fieldId, const autil::StringView& value, bool isNull, const uint64_t* hashKey)
{
    for (auto it = buildInfoHolder->diskIndexers.begin(); it != buildInfoHolder->diskIndexers.end(); ++it) {
        docid_t baseId = it->first.first;
        uint32_t segmentDocCount = it->first.second;
        if (docId < baseId || docId >= baseId + segmentDocCount) {
            continue;
        }
        if (unlikely(it->second == nullptr)) {
            auto key = std::make_pair(baseId, segmentDocCount);
            auto segmentIter = buildInfoHolder->diskSegments.find(key);
            if (segmentIter == buildInfoHolder->diskSegments.end()) {
                return false;
            }
            indexlibv2::framework::Segment* segment = segmentIter->second;
            std::shared_ptr<DiskIndexerType> diskIndexer;
            auto st = IndexerOrganizerUtil::GetIndexer<DiskIndexerType, /*unused=*/DiskIndexerType>(
                segment, buildInfoHolder->indexConfig, &diskIndexer, /*dumpingMemIndexer=*/nullptr,
                /*buildingMemIndexer=*/nullptr);
            if (!st.IsOK()) {
                return false;
            }
            it->second = diskIndexer;
        }
        if (it->second != nullptr) {
            return it->second->UpdateField(docId - baseId, value, isNull, hashKey);
        } else {
            AUTIL_INTERVAL_LOG2(
                10, WARN, "Updating field[%d] with docId [%d] value [%s] to default value indexer is not supported.",
                fieldId, docId, value.to_string().c_str());
            return true;
        }
    }
    AUTIL_INTERVAL_LOG2(60, WARN,
                        "Unable to find disk segment in updating attribute, this is OK in offline build, but fatal in "
                        "online build");
    return true;
}

template <typename DiskIndexerType, typename MemIndexerType>
bool AttributeIndexerOrganizerUtil::UpdateFieldInDumpingIndexer(
    SingleAttributeBuildInfoHolder<DiskIndexerType, MemIndexerType>* buildInfoHolder, const docid_t docId,
    fieldid_t fieldId, const autil::StringView& value, bool isNull, const uint64_t* hashKey)
{
    for (auto it = buildInfoHolder->dumpingIndexers.begin(); it != buildInfoHolder->dumpingIndexers.end(); ++it) {
        docid_t baseId = it->first.first;
        uint32_t segmentDocCount = it->first.second;
        if (docId < baseId || docId >= baseId + segmentDocCount) {
            continue;
        }
        if (it->second != nullptr) {
            return it->second->UpdateField(docId - baseId, value, isNull, hashKey);
        } else {
            AUTIL_INTERVAL_LOG2(
                10, WARN, "Updating field[%d] with docId [%d] value [%s] to default value indexer is not supported.",
                fieldId, docId, value.to_string().c_str());
            return true;
        }
    }
    return false;
}

} // namespace indexlib::index
