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
#ifndef __INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H
#define __INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/customized_index/customized_index_field_encoder.h"
#include "indexlib/common/field_format/date/date_field_encoder.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class ExtendDocFieldsConvertor
{
public:
    const static uint32_t MAX_TOKEN_PER_SECTION;

public:
    ExtendDocFieldsConvertor(const config::IndexPartitionSchemaPtr& schema, regionid_t regionId = DEFAULT_REGIONID);
    ~ExtendDocFieldsConvertor();

private:
    ExtendDocFieldsConvertor(const ExtendDocFieldsConvertor&);
    ExtendDocFieldsConvertor& operator=(const ExtendDocFieldsConvertor&);

private:
    typedef common::AttributeConvertorPtr AttributeConvertorPtr;
    typedef std::vector<AttributeConvertorPtr> AttributeConvertorVector;

public:
    void convertIndexField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig);
    void convertAttributeField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig,
                               bool emptyFieldNotEncode = false);
    void convertSummaryField(const IndexlibExtendDocumentPtr& document, const config::FieldConfigPtr& fieldConfig);

private:
    void CreateIndexField(const config::FieldConfig& fieldConfig, const TokenizeFieldPtr& tokenizeField,
                          const ClassifiedDocumentPtr& classifiedDoc, IndexTokenizeField** indexTokenizeField,
                          bool* isNull);
    void transTokenizeFieldToField(const TokenizeFieldPtr& tokenizeField, IndexTokenizeField* field, fieldid_t fieldId,
                                   const ClassifiedDocumentPtr& classifiedDoc);
    void addModifiedTokens(fieldid_t fieldId, const IndexTokenizeField* tokenizeField, bool isNull,
                           const IndexTokenizeField* lastTokenizeField, bool lastIsNull,
                           const IndexDocumentPtr& indexDoc);

private:
    void init();
    void initAttrConvert();
    void initFieldTokenHasherTypeVector();
    std::string transTokenizeFieldToSummaryStr(const TokenizeFieldPtr& tokenizeField,
                                               const config::FieldConfigPtr& fieldConfig);

    bool addSection(IndexTokenizeField* field, TokenizeSection* tokenizeSection, autil::mem_pool::Pool* pool,
                    fieldid_t fieldId, const ClassifiedDocumentPtr& classifiedDoc, pos_t& lastTokenPos, pos_t& curPos);
    bool addToken(Section* indexSection, const AnalyzerToken* token, autil::mem_pool::Pool* pool, fieldid_t fieldId,
                  pos_t& lastTokenPos, pos_t& curPos);
    bool addHashToken(Section* indexSection, uint64_t hashKey, autil::mem_pool::Pool* pool, fieldid_t fieldId,
                      pospayload_t posPayload, pos_t& lastTokenPos, pos_t& curPos);

    template <typename EncoderPtr>
    void addSectionTokens(const EncoderPtr& encoder, IndexTokenizeField* field, TokenizeSection* tokenizeSection,
                          autil::mem_pool::Pool* pool, fieldid_t fieldId, const ClassifiedDocumentPtr& classifiedDoc);
    template <typename EncoderPtr>
    bool supportMultiToken(const EncoderPtr& encoder) const
    {
        return false;
    }

private:
    AttributeConvertorVector _attrConvertVec;
    std::vector<index::TokenHasher> _fieldIdToTokenHasher;
    config::IndexPartitionSchemaPtr _schema;
    regionid_t _regionId;
    std::shared_ptr<indexlib::index::SpatialFieldEncoder> _spatialFieldEncoder;
    std::shared_ptr<indexlib::common::DateFieldEncoder> _dateFieldEncoder;
    std::shared_ptr<indexlib::common::RangeFieldEncoder> _rangeFieldEncoder;
    common::CustomizedIndexFieldEncoderPtr _customizedIndexFieldEncoder;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExtendDocFieldsConvertor);

template <>
inline bool ExtendDocFieldsConvertor::supportMultiToken<std::shared_ptr<indexlib::common::RangeFieldEncoder>>(
    const std::shared_ptr<indexlib::common::RangeFieldEncoder>& encoder) const
{
    return true;
}

template <typename EncoderPtr>
void ExtendDocFieldsConvertor::addSectionTokens(const EncoderPtr& encoder, IndexTokenizeField* field,
                                                TokenizeSection* tokenizeSection, autil::mem_pool::Pool* pool,
                                                fieldid_t fieldId, const ClassifiedDocumentPtr& classifiedDoc)
{
    uint32_t tokenCount = tokenizeSection->getTokenCount();
    if (tokenCount != 1 && !supportMultiToken(encoder)) {
        ERROR_COLLECTOR_LOG(DEBUG, "field [%s] content illegal, dropped",
                            _schema->GetFieldConfig(fieldId)->GetFieldName().c_str());
        return;
    }

    if (tokenCount > IndexTokenizeField::MAX_SECTION_PER_FIELD) {
        AUTIL_LOG(WARN,
                  "token number [%u] over MAX_SECTION_PER_FIELD, "
                  "redundant token will be ignored.",
                  tokenCount);
    }

    TokenizeSection::Iterator it = tokenizeSection->createIterator();
    do {
        const std::string& tokenStr = (*it)->getNormalizedText();
        std::vector<dictkey_t> dictKeys;
        encoder->Encode(fieldId, tokenStr, dictKeys);

        uint32_t leftTokenCount = dictKeys.size();
        Section* indexSection = classifiedDoc->createSection(field, leftTokenCount, 0);
        if (indexSection == NULL) {
            IE_LOG(DEBUG, "Failed to create new section.");
            return;
        }
        section_len_t nowSectionLen = 0;
        section_len_t maxSectionLen = classifiedDoc->getMaxSectionLenght();
        for (size_t i = 0; i < dictKeys.size(); i++) {
            if (nowSectionLen + 1 >= maxSectionLen) {
                indexSection->SetLength(nowSectionLen + 1);
                nowSectionLen = 0;
                indexSection = classifiedDoc->createSection(field, leftTokenCount, 0);
                if (indexSection == NULL) {
                    IE_LOG(DEBUG, "Failed to create new section.");
                    return;
                }
            }
            pos_t curPosition = 0;
            pos_t lastPosition = 0;
            addHashToken(indexSection, dictKeys[i], pool, fieldId, 0, lastPosition, curPosition);
            nowSectionLen++;
            leftTokenCount--;
        }
    } while (it.next());
}

}} // namespace indexlib::document

#endif //__INDEXLIB_DOCUMENT_EXTEND_DOC_FIELDS_CONVERTOR_H
