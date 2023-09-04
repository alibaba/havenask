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
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalExtendDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/date/DateFieldEncoder.h"
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class AttributeConfig;
}

namespace indexlibv2 { namespace document {

class ExtendDocFieldsConvertor
{
public:
    const static uint32_t MAX_TOKEN_PER_SECTION;

public:
    ExtendDocFieldsConvertor(const std::shared_ptr<config::ITabletSchema>& schema);
    virtual ~ExtendDocFieldsConvertor();
    void init();

private:
    ExtendDocFieldsConvertor(const ExtendDocFieldsConvertor&);
    ExtendDocFieldsConvertor& operator=(const ExtendDocFieldsConvertor&);

private:
    typedef std::shared_ptr<indexlibv2::index::AttributeConvertor> AttributeConvertorPtr;
    typedef std::vector<AttributeConvertorPtr> AttributeConvertorVector;

public:
    void convertIndexField(const NormalExtendDocument* document,
                           const std::shared_ptr<config::FieldConfig>& fieldConfig);
    void convertAttributeField(const NormalExtendDocument* document,
                               const std::shared_ptr<config::FieldConfig>& fieldConfig,
                               bool emptyFieldNotEncode = false);
    void convertSummaryField(const NormalExtendDocument* document,
                             const std::shared_ptr<config::FieldConfig>& fieldConfig);

protected:
    virtual std::vector<std::shared_ptr<index::AttributeConfig>> GetAttributeConfigs() const;

private:
    void CreateIndexField(const config::FieldConfig& fieldConfig,
                          const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                          const std::shared_ptr<ClassifiedDocument>& classifiedDoc,
                          indexlib::document::IndexTokenizeField** indexTokenizeField, bool* isNull);
    void transTokenizeFieldToField(const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                                   indexlib::document::IndexTokenizeField* field, fieldid_t fieldId,
                                   const std::shared_ptr<ClassifiedDocument>& classifiedDoc);
    void addModifiedTokens(fieldid_t fieldId, const indexlib::document::IndexTokenizeField* tokenizeField, bool isNull,
                           const indexlib::document::IndexTokenizeField* lastTokenizeField, bool lastIsNull,
                           const std::shared_ptr<indexlib::document::IndexDocument>& indexDoc);

private:
    void initAttrConvert();
    void initFieldTokenHasherTypeVector();
    std::string transTokenizeFieldToSummaryStr(const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                                               const std::shared_ptr<config::FieldConfig>& fieldConfig);

    bool addSection(indexlib::document::IndexTokenizeField* field, indexlib::document::TokenizeSection* tokenizeSection,
                    autil::mem_pool::Pool* pool, fieldid_t fieldId,
                    const std::shared_ptr<ClassifiedDocument>& classifiedDoc, pos_t& lastTokenPos, pos_t& curPos);
    bool addToken(indexlib::document::Section* indexSection, const indexlib::document::AnalyzerToken* token,
                  autil::mem_pool::Pool* pool, fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos,
                  indexlib::document::IndexDocument::TermOriginValueMap& termOriginValueMap);
    bool addHashToken(indexlib::document::Section* indexSection, uint64_t hashKey, autil::mem_pool::Pool* pool,
                      fieldid_t fieldId, pospayload_t posPayload, pos_t& lastTokenPos, pos_t& curPos);

    template <typename EncoderPtr>
    void addSectionTokens(const EncoderPtr& encoder, indexlib::document::IndexTokenizeField* field,
                          indexlib::document::TokenizeSection* tokenizeSection, autil::mem_pool::Pool* pool,
                          fieldid_t fieldId, const std::shared_ptr<ClassifiedDocument>& classifiedDoc);
    template <typename EncoderPtr>
    bool supportMultiToken(const EncoderPtr& encoder) const
    {
        return false;
    }
    void addTermOriginValueMap(const std::string& termStr, uint64_t hashKey, fieldid_t fieldId,
                               indexlib::document::IndexDocument::TermOriginValueMap& termOriginValueMap) const;

protected:
    std::shared_ptr<config::ITabletSchema> _schema;

private:
    AttributeConvertorVector _attrConvertVec;
    std::vector<indexlib::index::TokenHasher> _fieldIdToTokenHasher;
    std::shared_ptr<indexlib::index::SpatialFieldEncoder> _spatialFieldEncoder;
    std::shared_ptr<indexlib::index::DateFieldEncoder> _dateFieldEncoder;
    std::shared_ptr<indexlib::index::RangeFieldEncoder> _rangeFieldEncoder;
    std::map<std::string, std::shared_ptr<indexlibv2::config::InvertedIndexConfig>> _indexName2InvertedIndexConfig;

private:
    AUTIL_LOG_DECLARE();
};

template <>
inline bool ExtendDocFieldsConvertor::supportMultiToken<std::shared_ptr<indexlib::index::RangeFieldEncoder>>(
    const std::shared_ptr<indexlib::index::RangeFieldEncoder>& encoder) const
{
    return true;
}

}} // namespace indexlibv2::document
