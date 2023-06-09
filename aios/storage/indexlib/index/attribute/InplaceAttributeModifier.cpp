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
#include "indexlib/index/attribute/InplaceAttributeModifier.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/UpdateFieldExtractor.h"
#include "indexlib/index/common/IndexerOrganizerUtil.h"
#include "indexlib/index/primary_key/Common.h"

namespace indexlibv2::index {
namespace {
using indexlib::index::AttributeIndexerOrganizerUtil;
}
AUTIL_LOG_SETUP(indexlib.index, InplaceAttributeModifier);

InplaceAttributeModifier::InplaceAttributeModifier(const std::shared_ptr<config::TabletSchema>& schema)
    : AttributeModifier(schema)
{
}

Status InplaceAttributeModifier::Update(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            auto normalDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>((*docBatch)[i]);
            if (normalDoc == nullptr) {
                continue;
            }
            if (UPDATE_FIELD != normalDoc->GetDocOperateType()) {
                continue;
            }
            auto attrDoc = normalDoc->GetAttributeDocument();
            if (attrDoc == nullptr) {
                continue;
            }
            auto docId = attrDoc->GetDocId();
            if (INVALID_DOCID == docId) {
                continue;
            }
            if (!UpdateAttrDoc(docId, attrDoc.get())) {
                AUTIL_LOG(ERROR, "update attr doc failed, docId[%d]", docId);
                return Status::InternalError("update attr doc failed, docId:", docId);
            }
        }
    }
    return Status::OK();
}

Status InplaceAttributeModifier::Init(const framework::TabletData& tabletData)
{
    indexlib::index::IndexerOrganizerMeta::InitIndexerOrganizerMeta(tabletData, &_indexerOrganizerMeta);

    auto attributeConfigs = _schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    if (attributeConfigs.empty()) {
        AUTIL_LOG(INFO, "Attribute field does not exist.");
        return Status::OK();
    }
    for (const auto& indexConfig : attributeConfigs) {
        auto attributeConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        const fieldid_t fieldId = attributeConfig->GetFieldId();

        docid_t baseDocId = 0;
        auto slice = tabletData.CreateSlice();
        for (auto it = slice.begin(); it != slice.end(); it++) {
            framework::Segment* segment = it->get();
            auto segStatus = segment->GetSegmentStatus();
            auto docCount = segment->GetSegmentInfo()->docCount;
            if (segStatus == framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
                continue; // there is no document in current segment, so do nothing
            }
            std::shared_ptr<index::AttributeDiskIndexer> diskIndexer = nullptr;
            std::shared_ptr<index::AttributeMemIndexer> dumpingMemIndexer = nullptr;
            std::shared_ptr<index::AttributeMemIndexer> buildingMemIndexer = nullptr;
            Status st = indexlib::index::IndexerOrganizerUtil::GetIndexer<index::AttributeDiskIndexer,
                                                                          index::AttributeMemIndexer>(
                segment, attributeConfig, &diskIndexer, &dumpingMemIndexer, &buildingMemIndexer);
            RETURN_STATUS_DIRECTLY_IF_ERROR(st);
            if (diskIndexer == nullptr && dumpingMemIndexer == nullptr && buildingMemIndexer == nullptr) {
                continue;
            }

            if (_buildInfoHolders.find(fieldId) == _buildInfoHolders.end()) {
                indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer, index::AttributeMemIndexer>
                    buildInfoHolder;
                auto st =
                    AttributeIndexerOrganizerUtil::CreateSingleAttributeBuildInfoHolder<index::AttributeDiskIndexer,
                                                                                        index::AttributeMemIndexer>(
                        indexConfig, attributeConfig, &buildInfoHolder);
                RETURN_STATUS_DIRECTLY_IF_ERROR(st);
                _buildInfoHolders[fieldId] = buildInfoHolder;
            }

            auto key = std::pair<docid_t, uint32_t>(baseDocId, docCount);
            if (segStatus == framework::Segment::SegmentStatus::ST_BUILT) {
                _buildInfoHolders.at(fieldId).diskIndexers[key] = diskIndexer;
            } else if (segStatus == framework::Segment::SegmentStatus::ST_DUMPING) {
                _buildInfoHolders.at(fieldId).dumpingIndexers[key] = dumpingMemIndexer;
            } else if (segStatus == framework::Segment::SegmentStatus::ST_BUILDING) {
                assert(_buildInfoHolders.at(fieldId).buildingIndexer == nullptr);
                _buildInfoHolders.at(fieldId).buildingIndexer = buildingMemIndexer;
            }
            baseDocId += docCount;
        }
    }
    ValidateNullValue(_buildInfoHolders);
    return Status::OK();
}

bool InplaceAttributeModifier::UpdateAttrDoc(docid_t docId, indexlib::document::AttributeDocument* attrDoc)
{
    const auto& pkConfigs = _schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR);
    std::shared_ptr<config::IIndexConfig> pkConfig;
    if (pkConfigs.size() > 0) {
        pkConfig = pkConfigs[0];
    }
    const auto& attrConfigs = _schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    const auto& fields = _schema->GetFieldConfigs();
    assert(!fields.empty());

    UpdateFieldExtractor extractor;
    extractor.Init(pkConfig, attrConfigs, fields);
    if (!extractor.LoadFieldsFromDoc(attrDoc)) {
        return false;
    }

    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        const autil::StringView& value = iter.Next(fieldId, isNull);

        auto iter = _buildInfoHolders.find(fieldId);
        assert(iter != _buildInfoHolders.end());
        indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer, index::AttributeMemIndexer>*
            buildInfoHolder = &(iter->second);
        if (!UpdateAttribute(docId, buildInfoHolder, value, isNull)) {
            return false;
        }
    }
    return true;
}

bool InplaceAttributeModifier::UpdateAttribute(
    docid_t docId,
    indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer, index::AttributeMemIndexer>*
        buildInfoHolder,
    const autil::StringView& value, const bool isNull)
{
    const indexlibv2::config::AttributeConfig* attributeConfig = buildInfoHolder->attributeConfig.get();
    assert(attributeConfig != nullptr);
    auto packAttributeConfig = attributeConfig->GetPackAttributeConfig();
    if (packAttributeConfig != nullptr) {
        AUTIL_LOG(ERROR, "Pack attribute does not support update yet.");
        return false;
    }
    if (isNull) {
        if (attributeConfig->IsMultiValue() || attributeConfig->GetFieldType() == ft_string) {
            AttrValueMeta meta = buildInfoHolder->attributeConvertor->Decode(buildInfoHolder->nullValue);
            AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, buildInfoHolder, meta.data,
                                                       /*isNull=*/true, &meta.hashKey);
        } else {
            AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, buildInfoHolder, value,
                                                       /*isNull=*/true,
                                                       /*hashKey=*/nullptr);
        }
    } else {
        AttrValueMeta meta = buildInfoHolder->attributeConvertor->Decode(value);
        AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, buildInfoHolder, meta.data,
                                                   /*isNull=*/false, &meta.hashKey);
    }
    return true;
}

bool InplaceAttributeModifier::UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value,
                                           bool isNull)
{
    auto iter = _buildInfoHolders.find(fieldId);
    assert(iter != _buildInfoHolders.end());
    indexlib::index::SingleAttributeBuildInfoHolder<index::AttributeDiskIndexer, index::AttributeMemIndexer>*
        buildInfoHolder = &(iter->second);
    return AttributeIndexerOrganizerUtil::UpdateField(docId, _indexerOrganizerMeta, buildInfoHolder, value, isNull);
}

void InplaceAttributeModifier::ValidateNullValue(
    const std::map<fieldid_t, indexlib::index::SingleAttributeBuildInfoHolder<
                                  index::AttributeDiskIndexer, index::AttributeMemIndexer>>& buildInfoHolders)
{
    std::string nullValue;
    for (const auto& pair : buildInfoHolders) {
        if (nullValue.empty()) {
            nullValue = pair.second.nullValue;
        }
        if (!nullValue.empty() && !pair.second.nullValue.empty()) {
            assert(nullValue == pair.second.nullValue);
        }
    }
}

} // namespace indexlibv2::index
