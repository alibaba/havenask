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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/TokenizeDocumentConvertor.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/field_format/date/DateFieldEncoder.h"
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"
#include "indexlib/index/inverted_index/InvertedIndexFields.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
class FieldConfig;
} // namespace indexlibv2::config
namespace indexlibv2::index {

class InvertedIndexFieldsParser : public document::IIndexFieldsParser
{
public:
    InvertedIndexFieldsParser();
    ~InvertedIndexFieldsParser();

public:
    Status Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                const std::shared_ptr<document::DocumentInitParam>& initParam) override;
    indexlib::util::PooledUniquePtr<document::IIndexFields>
    Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool, bool& hasFormatError) const override;
    indexlib::util::PooledUniquePtr<document::IIndexFields> Parse(autil::StringView serializedData,
                                                                  autil::mem_pool::Pool* pool) const override;

private:
    Status PrepareTokenizedConvertor(const std::shared_ptr<document::DocumentInitParam>& initParam);

    std::tuple<Status, std::shared_ptr<indexlib::document::TokenizeDocument>,
               std::shared_ptr<indexlib::document::TokenizeDocument>>
    ParseTokenized(const document::ExtendDocument& extendDoc) const;

    void CreateIndexField(const indexlibv2::config::FieldConfig& fieldConfig,
                          const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                          index::InvertedIndexFields* invertedIndexFields, autil::mem_pool::Pool* pool,
                          indexlib::document::IndexTokenizeField** indexTokenizeField, bool* isNull) const;

    void TransTokenizeFieldToField(const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                                   indexlib::document::IndexTokenizeField* field, fieldid_t fieldId,
                                   index::InvertedIndexFields* invertedIndexFields, autil::mem_pool::Pool* pool) const;

    indexlib::document::Section* CreateSection(indexlib::document::IndexTokenizeField* field, uint32_t tokenNum,
                                               section_weight_t sectionWeight) const;

    template <typename EncoderPtr>
    void AddSectionTokens(const EncoderPtr& encoder, indexlib::document::IndexTokenizeField* field,
                          indexlib::document::TokenizeSection* tokenizeSection, autil::mem_pool::Pool* pool,
                          fieldid_t fieldId, index::InvertedIndexFields* invertedIndexFields) const;

    template <typename EncoderPtr>
    bool SupportMultiToken(const EncoderPtr& encoder) const;

    bool AddHashToken(indexlib::document::Section* indexSection, uint64_t hashKey, autil::mem_pool::Pool* pool,
                      pospayload_t posPayload, pos_t& lastTokenPos, pos_t& curPos) const;

    bool AddToken(indexlib::document::Section* indexSection, const indexlib::document::AnalyzerToken* token,
                  autil::mem_pool::Pool* pool, fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos) const;

    bool AddSection(indexlib::document::IndexTokenizeField* field, indexlib::document::TokenizeSection* tokenizeSection,
                    autil::mem_pool::Pool* pool, fieldid_t fieldId, index::InvertedIndexFields* invertedIndexFields,
                    pos_t& lastTokenPos, pos_t& curPos) const;

private:
    std::map<fieldid_t, std::shared_ptr<config::FieldConfig>> _fieldIdToFieldConfig;
    std::vector<indexlib::index::TokenHasher> _fieldIdToTokenHasher;
    std::vector<std::shared_ptr<config::InvertedIndexConfig>> _indexConfigs;
    std::shared_ptr<document::TokenizeDocumentConvertor> _tokenizeDocConvertor;

    std::shared_ptr<indexlib::index::SpatialFieldEncoder> _spatialFieldEncoder;
    std::shared_ptr<indexlib::index::DateFieldEncoder> _dateFieldEncoder;
    std::shared_ptr<indexlib::index::RangeFieldEncoder> _rangeFieldEncoder;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
