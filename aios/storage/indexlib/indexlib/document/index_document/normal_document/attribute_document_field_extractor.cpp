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
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, AttributeDocumentFieldExtractor);

AttributeDocumentFieldExtractor::AttributeDocumentFieldExtractor() {}

AttributeDocumentFieldExtractor::~AttributeDocumentFieldExtractor() {}

bool AttributeDocumentFieldExtractor::Init(const AttributeSchemaPtr& attributeSchema)
{
    if (!attributeSchema) {
        return false;
    }
    mAttrSchema = attributeSchema;
    size_t packAttrCount = attributeSchema->GetPackAttributeCount();
    for (size_t i = 0; i < packAttrCount; i++) {
        const PackAttributeConfigPtr packAttrConfig = attributeSchema->GetPackAttributeConfig(packattrid_t(i));
        assert(packAttrConfig);

        PackAttributeFormatterPtr formatter(new PackAttributeFormatter());
        if (!formatter->Init(packAttrConfig)) {
            return false;
        }
        mPackFormatters.push_back(formatter);
    }
    return true;
}

StringView AttributeDocumentFieldExtractor::GetField(const AttributeDocumentPtr& attrDoc, fieldid_t fid, Pool* pool)
{
    if (!attrDoc) {
        return StringView::empty_instance();
    }

    if (attrDoc->HasField(fid)) {
        return attrDoc->GetField(fid);
    }

    // find field in pack attribute
    AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfigByFieldId(fid);
    if (!attrConfig) {
        IE_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fid);
        ERROR_COLLECTOR_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fid);
        return StringView::empty_instance();
    }

    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (!packAttrConfig) {
        // empty single value field will trigger this log
        IE_LOG(DEBUG,
               "get field[%d] failed, "
               "it is not in attributeDocument.",
               fid);
        ERROR_COLLECTOR_LOG(ERROR,
                            "get field[%d] failed, "
                            "it is not in attributeDocument.",
                            fid);
        return StringView::empty_instance();
    }

    packattrid_t packId = packAttrConfig->GetPackAttrId();
    const PackAttributeFormatterPtr& formatter = mPackFormatters[packId];
    assert(formatter);
    const StringView& packValue = attrDoc->GetPackField(packId);
    if (packValue.empty()) {
        IE_LOG(ERROR,
               "get field[%d] failed, "
               "due to pack attribute[%ud] is empty.",
               fid, packId);
        ERROR_COLLECTOR_LOG(ERROR,
                            "get field[%d] failed, "
                            "due to pack attribute[%ud] is empty.",
                            fid, packId);
        return StringView::empty_instance();
    }

    // rawValue is un-encoded fieldValue
    auto rawField = formatter->GetFieldValueFromPackedValue(packValue, attrConfig->GetAttrId());
    AttributeConvertorPtr convertor = formatter->GetAttributeConvertorByAttrId(attrConfig->GetAttrId());
    assert(pool);
    uint64_t dummyKey = uint64_t(-1);
    if (rawField.isFixedValueLen || rawField.hasCountInValueStr) {
        common::AttrValueMeta attrValueMeta(dummyKey, rawField.valueStr);
        return convertor->EncodeFromAttrValueMeta(attrValueMeta, pool);
    }

    // in pack value store without count, need encode count
    size_t bufferSize =
        rawField.valueStr.size() + autil::MultiValueFormatter::getEncodedCountLength(rawField.valueCount);
    char* buffer = (char*)pool->allocate(bufferSize);
    size_t countLen = autil::MultiValueFormatter::encodeCount(rawField.valueCount, buffer, bufferSize);
    memcpy(buffer + countLen, rawField.valueStr.data(), rawField.valueStr.size());
    common::AttrValueMeta attrValueMeta(dummyKey, autil::StringView(buffer, bufferSize));
    return convertor->EncodeFromAttrValueMeta(attrValueMeta, pool);
}

}} // namespace indexlib::document
