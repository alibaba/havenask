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
#include "indexlib/index/inverted_index/InvertedIndexFieldsParser.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentInitParam.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/IndexRawField.h"
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"
#include "indexlib/index/inverted_index/InvertedIndexFields.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexFieldsParser);

using config::InvertedIndexConfig;

InvertedIndexFieldsParser::InvertedIndexFieldsParser() {}

InvertedIndexFieldsParser::~InvertedIndexFieldsParser() {}

Status InvertedIndexFieldsParser::Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                                       const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    for (const auto& indexConfig : indexConfigs) {
        auto typedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
        if (!typedIndexConfig) {
            RETURN_STATUS_ERROR(Status::InvalidArgs, "cast to inverted index config failed");
        }

        for (const auto& fieldConfig : indexConfig->GetFieldConfigs()) {
            assert(fieldConfig);
            _fieldIdToFieldConfig[fieldConfig->GetFieldId()] = fieldConfig;
        }
        _indexConfigs.push_back(typedIndexConfig);
    }

    for (const auto& [fieldId, fieldConfig] : _fieldIdToFieldConfig) {
        const auto& jsonMap = fieldConfig->GetUserDefinedParam();
        const auto& kvMap = indexlib::util::ConvertFromJsonMap(jsonMap);
        if (fieldId >= _fieldIdToTokenHasher.size()) {
            _fieldIdToTokenHasher.resize(fieldId + 1);
        }
        _fieldIdToTokenHasher[fieldId] = indexlib::index::TokenHasher(kvMap, fieldConfig->GetFieldType());
    }
    RETURN_IF_STATUS_ERROR(PrepareTokenizedConvertor(initParam), "");

    _spatialFieldEncoder.reset(new indexlib::index::SpatialFieldEncoder(indexConfigs));
    _dateFieldEncoder.reset(new indexlib::index::DateFieldEncoder(indexConfigs));
    _rangeFieldEncoder.reset(new indexlib::index::RangeFieldEncoder(indexConfigs));
    return Status::OK();
}

Status
InvertedIndexFieldsParser::PrepareTokenizedConvertor(const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    std::string needConvertor;
    if (initParam && initParam->GetValue("tokenize_on_parse", needConvertor) && needConvertor == "true") {
        AUTIL_LOG(INFO, "will tokenize on parse");
        _tokenizeDocConvertor = std::make_shared<document::TokenizeDocumentConvertor>();
        std::vector<std::shared_ptr<config::FieldConfig>> fieldConfigs;
        for (auto [_, fieldConfig] : _fieldIdToFieldConfig) {
            fieldConfigs.push_back(fieldConfig);
        }
        auto status = _tokenizeDocConvertor->Init(fieldConfigs, nullptr);
        RETURN_IF_STATUS_ERROR(status, "init tokenized doc convertor failed");
    }
    AUTIL_LOG(INFO, "will not tokenize on parse");
    return Status::OK();
}

std::tuple<Status, std::shared_ptr<indexlib::document::TokenizeDocument>,
           std::shared_ptr<indexlib::document::TokenizeDocument>>
InvertedIndexFieldsParser::ParseTokenized(const document::ExtendDocument& extendDoc) const
{
    if (!_tokenizeDocConvertor) {
        return {Status::OK(), nullptr, nullptr};
    }

    if (extendDoc.GetResource<indexlib::document::TokenizeDocument>("tokenized_document")) {
        RETURN3_IF_STATUS_ERROR(Status::Exist(), nullptr, nullptr, "tokenized_document is existed");
    }
    if (extendDoc.GetResource<indexlib::document::TokenizeDocument>("last_tokenized_document")) {
        RETURN3_IF_STATUS_ERROR(Status::Exist(), nullptr, nullptr, "last_tokenized_document is existed");
    }

    auto tokenDocument = std::make_shared<indexlib::document::TokenizeDocument>();
    auto lastTokenDocument = std::make_shared<indexlib::document::TokenizeDocument>();
    std::map<fieldid_t, std::string> fieldAnalyzerNameMap;
    auto rawDoc = extendDoc.GetRawDocument();
    assert(rawDoc);
    auto status = _tokenizeDocConvertor->Convert(rawDoc.get(), fieldAnalyzerNameMap, tokenDocument, lastTokenDocument);
    RETURN3_IF_STATUS_ERROR(status, nullptr, nullptr, "convert failed");
    return {Status::OK(), tokenDocument, lastTokenDocument};
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
InvertedIndexFieldsParser::Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool,
                                 bool& hasFormatError) const
{
    hasFormatError = false;
    auto [status, tokenizeDoc, lastTokenizeDoc] = ParseTokenized(extendDoc);
    if (!status.IsOK()) {
        AUTIL_LOG(INFO, "parse tokenized failed");
        return nullptr;
    }
    auto rawDoc = extendDoc.GetRawDocument();
    assert(rawDoc);

    auto inveredIndexFields = indexlib::util::MakePooledUniquePtr<InvertedIndexFields>(pool, pool);
    if (!tokenizeDoc) {
        tokenizeDoc = extendDoc.GetResource<indexlib::document::TokenizeDocument>("tokenized_document");
    }
    if (!lastTokenizeDoc) {
        lastTokenizeDoc = extendDoc.GetResource<indexlib::document::TokenizeDocument>("last_tokenized_document");
    }

    for (const auto& [fieldId, fieldConfig] : _fieldIdToFieldConfig) {
        // TODO test
        FieldType fieldType = fieldConfig->GetFieldType();
        if (fieldType == ft_raw) {
            const std::string& fieldValue = rawDoc->getField(fieldConfig->GetFieldName());
            if (fieldValue.empty()) {
                continue;
            }
            auto indexRawField = dynamic_cast<indexlib::document::IndexRawField*>(inveredIndexFields->createIndexField(
                fieldConfig->GetFieldId(), indexlib::document::Field::FieldTag::RAW_FIELD));
            assert(indexRawField);
            indexRawField->SetData(autil::MakeCString(fieldValue, pool));
            continue;
        }

        if (!tokenizeDoc) {
            continue;
        }
        const auto& tokenizeField = tokenizeDoc->getField(fieldId);
        const auto& lastTokenizeField = lastTokenizeDoc->getField(fieldId);
        if (!tokenizeField) {
            continue;
        }

        indexlib::document::IndexTokenizeField* indexTokenField = nullptr;
        bool isNull = false;
        CreateIndexField(*fieldConfig, tokenizeField, inveredIndexFields.get(), pool, &indexTokenField, &isNull);

        if (lastTokenizeField) {
            // TODO (远轫) 不支持 lastTokenizeField 即 倒排更新功能
            assert(false);
            AUTIL_LOG(ERROR, "not support lastTokenizeField in new mode");
            continue;
            // std::shared_ptr<ClassifiedDocument> lastClassifiedDoc(new ClassifiedDocument());
            // indexlib::document::IndexTokenizeField* lastIndexTokenField = nullptr;
            // bool lastIsNull = false;
            // CreateIndexField(*fieldConfig, lastTokenizeField, lastClassifiedDoc, &lastIndexTokenField, &lastIsNull);
            // addModifiedTokens(fieldId, indexTokenField, isNull, lastIndexTokenField, lastIsNull,
            //                   classifiedDoc->getIndexDocument());
        }
    }
    return inveredIndexFields;
}

void InvertedIndexFieldsParser::CreateIndexField(
    const indexlibv2::config::FieldConfig& fieldConfig,
    const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
    index::InvertedIndexFields* invertedIndexFields, autil::mem_pool::Pool* pool,
    indexlib::document::IndexTokenizeField** indexTokenizeField, bool* isNull) const
{
    fieldid_t fieldId = fieldConfig.GetFieldId();
    *isNull = false;
    *indexTokenizeField = nullptr;
    if (tokenizeField->isNull() && fieldConfig.IsEnableNullField()) {
        [[maybe_unused]] auto* field =
            invertedIndexFields->createIndexField(fieldId, indexlib::document::Field::FieldTag::NULL_FIELD);
        *isNull = true;
    } else {
        if (tokenizeField->isEmpty()) {
            return;
        }
        auto* field = invertedIndexFields->createIndexField(fieldId, indexlib::document::Field::FieldTag::TOKEN_FIELD);
        *indexTokenizeField = static_cast<indexlib::document::IndexTokenizeField*>(field);
        TransTokenizeFieldToField(tokenizeField, *indexTokenizeField, fieldId, invertedIndexFields, pool);
    }
}

void InvertedIndexFieldsParser::TransTokenizeFieldToField(
    const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
    indexlib::document::IndexTokenizeField* field, fieldid_t fieldId, index::InvertedIndexFields* invertedIndexFields,
    autil::mem_pool::Pool* pool) const
{
    indexlib::document::TokenizeField::Iterator it = tokenizeField->createIterator();
    if (it.isEnd()) {
        return;
    }
    if (_spatialFieldEncoder->IsSpatialIndexField(fieldId)) {
        AddSectionTokens(_spatialFieldEncoder, field, *it, pool, fieldId, invertedIndexFields);
        return;
    }
    if (_dateFieldEncoder->IsDateIndexField(fieldId)) {
        AddSectionTokens(_dateFieldEncoder, field, *it, pool, fieldId, invertedIndexFields);
        return;
    }
    if (_rangeFieldEncoder->IsRangeIndexField(fieldId)) {
        AddSectionTokens(_rangeFieldEncoder, field, *it, pool, fieldId, invertedIndexFields);
        return;
    }
    assert(tokenizeField);
    pos_t lastTokenPos = 0;
    pos_t curPos = -1;
    while (!it.isEnd()) {
        indexlib::document::TokenizeSection* section = *it;
        if (!AddSection(field, section, pool, fieldId, invertedIndexFields, lastTokenPos, curPos)) {
            return;
        }
        it.next();
    }
}

indexlib::document::Section* InvertedIndexFieldsParser::CreateSection(indexlib::document::IndexTokenizeField* field,
                                                                      uint32_t tokenNum,
                                                                      section_weight_t sectionWeight) const
{
    auto* indexSection = field->CreateSection(tokenNum);
    if (nullptr == indexSection) {
        return nullptr;
    }

    indexSection->SetWeight(sectionWeight);
    if (field->GetSectionCount() >= indexlib::document::Field::MAX_SECTION_PER_FIELD) {
        return nullptr;
    }
    return indexSection;
}

template <typename EncoderPtr>
bool InvertedIndexFieldsParser::SupportMultiToken(const EncoderPtr& encoder) const
{
    if constexpr (std::is_same<EncoderPtr, std::shared_ptr<indexlib::index::RangeFieldEncoder>>::value) {
        return true;
    }
    return false;
}

template <typename EncoderPtr>
void InvertedIndexFieldsParser::AddSectionTokens(const EncoderPtr& encoder,
                                                 indexlib::document::IndexTokenizeField* field,
                                                 indexlib::document::TokenizeSection* tokenizeSection,
                                                 autil::mem_pool::Pool* pool, fieldid_t fieldId,
                                                 index::InvertedIndexFields* invertedIndexFields) const
{
    uint32_t tokenCount = tokenizeSection->getTokenCount();
    if (tokenCount != 1 && !SupportMultiToken(encoder)) {
        AUTIL_LOG(DEBUG, "field [%s] content illegal, dropped",
                  _fieldIdToFieldConfig.at(fieldId)->GetFieldName().c_str());
        return;
    }

    if (tokenCount > indexlib::document::IndexTokenizeField::MAX_SECTION_PER_FIELD) {
        AUTIL_LOG(WARN,
                  "token number [%u] over MAX_SECTION_PER_FIELD, "
                  "redundant token will be ignored.",
                  tokenCount);
    }

    indexlib::document::TokenizeSection::Iterator it = tokenizeSection->createIterator();
    do {
        const std::string& tokenStr = (*it)->getNormalizedText();
        std::vector<dictkey_t> dictKeys;
        encoder->Encode(fieldId, tokenStr, dictKeys);

        uint32_t leftTokenCount = dictKeys.size();
        indexlib::document::Section* indexSection = CreateSection(field, leftTokenCount, 0);
        if (indexSection == NULL) {
            AUTIL_LOG(DEBUG, "Failed to create new section.");
            return;
        }
        section_len_t nowSectionLen = 0;
        section_len_t maxSectionLen = indexlib::document::Section::MAX_SECTION_LENGTH;
        for (size_t i = 0; i < dictKeys.size(); i++) {
            if (nowSectionLen + 1 >= maxSectionLen) {
                indexSection->SetLength(nowSectionLen + 1);
                nowSectionLen = 0;
                indexSection = CreateSection(field, leftTokenCount, 0);
                if (indexSection == NULL) {
                    AUTIL_LOG(DEBUG, "Failed to create new section.");
                    return;
                }
            }
            pos_t curPosition = 0;
            pos_t lastPosition = 0;
            AddHashToken(indexSection, dictKeys[i], pool, 0, lastPosition, curPosition);
            nowSectionLen++;
            leftTokenCount--;
        }
    } while (it.next());
}

bool InvertedIndexFieldsParser::AddHashToken(indexlib::document::Section* indexSection, uint64_t hashKey,
                                             autil::mem_pool::Pool* pool, pospayload_t posPayload, pos_t& lastTokenPos,
                                             pos_t& curPos) const
{
    indexlib::document::Token* indexToken = indexSection->CreateToken(hashKey);
    if (!indexToken) {
        curPos--;
        AUTIL_LOG(INFO, "token count overflow in one section, max %u",
                  indexlib::document::Section::MAX_TOKEN_PER_SECTION);
        return false;
    }
    indexToken->SetPosPayload(posPayload);
    indexToken->SetPosIncrement(curPos - lastTokenPos);
    lastTokenPos = curPos;
    return true;
}

bool InvertedIndexFieldsParser::AddToken(indexlib::document::Section* indexSection,
                                         const indexlib::document::AnalyzerToken* token, autil::mem_pool::Pool* pool,
                                         fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos) const

{
    if (token->isSpace() || token->isStopWord()) {
        // do nothing
        return true;
    }

    const std::string& text = token->getNormalizedText();
    const indexlib::index::TokenHasher& tokenHasher = _fieldIdToTokenHasher[fieldId];
    uint64_t hashKey;
    if (!tokenHasher.CalcHashKey(text, hashKey)) {
        return true;
    }
    return AddHashToken(indexSection, hashKey, pool, token->getPosPayLoad(), lastTokenPos, curPos);
}

bool InvertedIndexFieldsParser::AddSection(indexlib::document::IndexTokenizeField* field,
                                           indexlib::document::TokenizeSection* tokenizeSection,
                                           autil::mem_pool::Pool* pool, fieldid_t fieldId,
                                           index::InvertedIndexFields* invertedIndexFields, pos_t& lastTokenPos,
                                           pos_t& curPos) const
{
    // TODO: empty section
    uint32_t leftTokenCount = tokenizeSection->getTokenCount();
    indexlib::document::Section* indexSection =
        CreateSection(field, leftTokenCount, tokenizeSection->getSectionWeight());
    if (indexSection == NULL) {
        AUTIL_LOG(DEBUG, "Failed to create new section.");
        return false;
    }
    indexlib::document::TokenizeSection::Iterator it = tokenizeSection->createIterator();
    section_len_t nowSectionLen = 0;
    section_len_t maxSectionLen = indexlib::document::Section::MAX_SECTION_LENGTH;
    while (*it != NULL) {
        if ((*it)->isSpace() || (*it)->isDelimiter()) {
            it.nextBasic();
            leftTokenCount--;
            continue;
        }

        if (nowSectionLen + 1 >= maxSectionLen) {
            indexSection->SetLength(nowSectionLen + 1);
            nowSectionLen = 0;
            indexSection = CreateSection(field, leftTokenCount, tokenizeSection->getSectionWeight());
            if (indexSection == NULL) {
                AUTIL_LOG(DEBUG, "Failed to create new section.");
                return false;
            }
            curPos++;
        }
        curPos++;
        if (!AddToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
            break;
        }

        while (it.nextExtend()) {
            if (!AddToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
                break;
            }
        }
        nowSectionLen++;
        leftTokenCount--;
        it.nextBasic();
    }
    indexSection->SetLength(nowSectionLen + 1);
    curPos++;

    return true;
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
InvertedIndexFieldsParser::Parse(autil::StringView serializedData, autil::mem_pool::Pool* pool) const
{
    auto inveredIndexFields = indexlib::util::MakePooledUniquePtr<InvertedIndexFields>(pool, pool);
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.size(), pool);
    try {
        inveredIndexFields->deserialize(dataBuffer);
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "deserialize inverted fields failed: [%s]", e.what());
        return nullptr;
    }
    return inveredIndexFields;
}

} // namespace indexlibv2::index
