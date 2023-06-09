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
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"

using namespace std;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InplaceAttributeModifier);

InplaceAttributeModifier::InplaceAttributeModifier(const IndexPartitionSchemaPtr& schema,
                                                   util::BuildResourceMetrics* buildResourceMetrics)
    : AttributeModifier(schema, buildResourceMetrics)
{
}

InplaceAttributeModifier::~InplaceAttributeModifier() {}

void InplaceAttributeModifier::Init(const AttributeReaderContainerPtr& attrReaderContainer,
                                    const PartitionDataPtr& partitionData)
{
    _attributeReaderMap.resize(mSchema->GetFieldCount());
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            continue;
        }
        if (!attrConfig->IsAttributeUpdatable()) {
            continue;
        }

        AttributeReaderPtr attrReader = attrReaderContainer->GetAttributeReader(attrConfig->GetAttrName());
        assert(attrReader);
        fieldid_t fieldId = attrConfig->GetFieldId();
        _attributeReaderMap[fieldId] = attrReader;
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    _packIdToPackFields.resize(packAttrCount);
    _packAttributeReaderMap.resize(packAttrCount);
    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++) {
        const auto& packAttrConfig = *packIter;
        PackAttributeReaderPtr packAttrReader;
        packattrid_t packId = packAttrConfig->GetPackAttrId();
        packAttrReader = attrReaderContainer->GetPackAttributeReader(packId);
        assert(packAttrReader);
        packAttrReader->InitBuildResourceMetricsNode(mBuildResourceMetrics);
        _packAttributeReaderMap[packId] = packAttrReader;
    }
    InitPackAttributeUpdateBitmap(partitionData);
}

AttributeSegmentReaderPtr InplaceAttributeModifier::GetAttributeSegmentReader(fieldid_t fieldId,
                                                                              docid_t segBaseDocId) const
{
    if ((size_t)fieldId >= _attributeReaderMap.size()) {
        IE_LOG(ERROR, "INVALID fieldId[%u]", fieldId);
        return AttributeSegmentReaderPtr();
    }
    const AttributeReaderPtr& attrReader = _attributeReaderMap[fieldId];
    if (!attrReader) {
        IE_LOG(ERROR, "AttributeReader is NULL for field[%u]", fieldId);
        return AttributeSegmentReaderPtr();
    }
    AttributeSegmentReaderPtr segReader = attrReader->GetSegmentReader(segBaseDocId);
    if (!segReader) {
        IE_LOG(ERROR, "No matching SegmentReader for docid[%d]", segBaseDocId);
    }
    return segReader;
}

AttributeReaderPtr InplaceAttributeModifier::GetAttributeReader(const config::AttributeConfigPtr& attrConfig)
{
    assert(attrConfig->GetFieldId() < _attributeReaderMap.size());
    return _attributeReaderMap[attrConfig->GetFieldId()];
}

PackAttributeReaderPtr InplaceAttributeModifier::GetPackAttributeReader(packattrid_t packAttrId) const
{
    if ((size_t)packAttrId >= _packAttributeReaderMap.size()) {
        IE_LOG(ERROR, "Invalid PackAttrId[%u], return NULL PackAttributeReader", packAttrId);
        return PackAttributeReaderPtr();
    }
    if (!_packAttributeReaderMap[packAttrId]) {
        IE_LOG(ERROR, "no matching PackAttributeReader for packAttrId[%u]", packAttrId);
    }
    return _packAttributeReaderMap[packAttrId];
}

bool InplaceAttributeModifier::Update(docid_t docId, const AttributeDocumentPtr& attrDoc)
{
    UpdateFieldExtractor extractor(mSchema);
    if (!extractor.Init(attrDoc)) {
        return false;
    }
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    assert(attrSchema);
    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        const StringView& value = iter.Next(fieldId, isNull);
        const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
        assert(attrConfig);
        PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
        if (packAttrConfig) {
            if (packAttrConfig->IsDisable()) {
                continue;
            }
            _packIdToPackFields[packAttrConfig->GetPackAttrId()].push_back(make_pair(attrConfig->GetAttrId(), value));

            const AttributeUpdateBitmapPtr& packAttrUpdateBitmap =
                mPackUpdateBitmapVec[packAttrConfig->GetPackAttrId()];
            assert(packAttrUpdateBitmap);
            if (unlikely(!packAttrUpdateBitmap)) {
                continue;
            }
            packAttrUpdateBitmap->Set(docId);
        } else {
            const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
            assert(convertor);
            if (isNull) {
                UpdateField(docId, fieldId, StringView::empty_instance(), true);
            } else {
                AttrValueMeta meta = convertor->Decode(value);
                UpdateField(docId, fieldId, meta.data, false);
            }
        }
    }
    UpdateInPackFields(docId);
    return true;
}

void InplaceAttributeModifier::UpdateAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc,
                                               attrid_t attrId)
{
    const AttributeConfigPtr& attrConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(attrId);
    assert(attrDoc && attrConfig && attrConfig->IsNormal() && !attrConfig->GetPackAttributeConfig());

    bool isNull = false;
    autil::StringView value;
    if (!UpdateFieldExtractor::GetFieldValue(mSchema, attrDoc, attrConfig->GetFieldId(), &isNull, &value)) {
        return;
    }
    if (isNull) {
        UpdateField(docId, attrConfig->GetFieldId(), StringView::empty_instance(), true);
    } else {
        const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[attrConfig->GetFieldId()];
        assert(convertor);
        AttrValueMeta meta = convertor->Decode(value);
        UpdateField(docId, attrConfig->GetFieldId(), meta.data, false);
    }
}

void InplaceAttributeModifier::UpdatePackAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc,
                                                   packattrid_t packAttrId)
{
    const PackAttributeConfigPtr& packAttrConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(packAttrId);
    assert(attrDoc && packAttrConfig && !packAttrConfig->IsDisable());

    for (const AttributeConfigPtr& attrConfig : packAttrConfig->GetAttributeConfigVec()) {
        if (!attrConfig->IsNormal()) {
            continue;
        }
        bool isNull = false;
        autil::StringView value;
        if (!UpdateFieldExtractor::GetFieldValue(mSchema, attrDoc, attrConfig->GetFieldId(), &isNull, &value)) {
            continue;
        }
        _packIdToPackFields[packAttrId].push_back(make_pair(attrConfig->GetAttrId(), value));

        const AttributeUpdateBitmapPtr& packAttrUpdateBitmap = mPackUpdateBitmapVec[packAttrId];
        assert(packAttrUpdateBitmap);
        if (unlikely(!packAttrUpdateBitmap)) {
            continue;
        }
        packAttrUpdateBitmap->Set(docId);
    }
    UpdateInPackField(docId, packAttrId);
}

void InplaceAttributeModifier::UpdateInPackFields(docid_t docId)
{
    assert(_packAttributeReaderMap.size() == _packIdToPackFields.size());
    for (size_t i = 0; i < _packIdToPackFields.size(); ++i) {
        UpdateInPackField(docId, i);
    }
}

void InplaceAttributeModifier::UpdateInPackField(docid_t docId, packattrid_t packAttrId)
{
    if (_packIdToPackFields[packAttrId].empty()) {
        return;
    }

    const PackAttributeReaderPtr& packAttrReader = _packAttributeReaderMap[packAttrId];
    assert(packAttrReader);
    packAttrReader->UpdatePackFields(docId, _packIdToPackFields[packAttrId], true);
    _packIdToPackFields[packAttrId].clear();
}

bool InplaceAttributeModifier::UpdateField(docid_t docId, fieldid_t fieldId, const StringView& value, bool isNull)
{
    assert((size_t)fieldId < _attributeReaderMap.size());
    const AttributeReaderPtr& attrReader = _attributeReaderMap[fieldId];
    if (!attrReader) {
        return false;
    }
    return attrReader->UpdateField(docId, (uint8_t*)value.data(), value.size(), isNull);
}

bool InplaceAttributeModifier::UpdatePackField(docid_t docId, packattrid_t packAttrId, const StringView& value)
{
    assert((size_t)packAttrId < _packAttributeReaderMap.size());
    const PackAttributeReaderPtr& packAttrReader = _packAttributeReaderMap[packAttrId];
    if (!packAttrReader) {
        return false;
    }
    return packAttrReader->UpdateField(docId, (uint8_t*)value.data(), value.size());
}

void InplaceAttributeModifier::Dump(const DirectoryPtr& dir)
{
    DirectoryPtr attrDir = dir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DumpPackAttributeUpdateInfo(attrDir);
}
}} // namespace indexlib::index
