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
#include "indexlib/document/normal/ExtendDocFieldsConvertor.h"

#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/date/DateFieldEncoder.h"
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, ExtendDocFieldsConvertor);

const uint32_t ExtendDocFieldsConvertor::MAX_TOKEN_PER_SECTION = indexlib::document::Section::MAX_TOKEN_PER_SECTION;

ExtendDocFieldsConvertor::ExtendDocFieldsConvertor(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
    : _schema(schema)
{
}

ExtendDocFieldsConvertor::~ExtendDocFieldsConvertor() {}

void ExtendDocFieldsConvertor::convertIndexField(const NormalExtendDocument* document,
                                                 const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const auto& classifiedDoc = document->getClassifiedDocument();

    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        const std::string& fieldValue = document->getRawDocument()->getField(fieldConfig->GetFieldName());
        if (fieldValue.empty()) {
            return;
        }
        auto indexRawField = dynamic_cast<indexlib::document::IndexRawField*>(
            classifiedDoc->createIndexField(fieldConfig->GetFieldId(), indexlib::document::Field::FieldTag::RAW_FIELD));
        assert(indexRawField);
        indexRawField->SetData(autil::MakeCString(fieldValue, classifiedDoc->getPool()));
        return;
    }
    const auto& tokenizeDoc = document->getTokenizeDocument();
    const auto& lastTokenizeDoc = document->getLastTokenizeDocument();
    const auto& tokenizeField = tokenizeDoc->getField(fieldId);
    const auto& lastTokenizeField = lastTokenizeDoc->getField(fieldId);
    if (!tokenizeField) {
        return;
    }

    indexlib::document::IndexTokenizeField* indexTokenField = nullptr;
    bool isNull = false;
    CreateIndexField(*fieldConfig, tokenizeField, classifiedDoc, &indexTokenField, &isNull);

    if (lastTokenizeField) {
        std::shared_ptr<ClassifiedDocument> lastClassifiedDoc(new ClassifiedDocument());
        indexlib::document::IndexTokenizeField* lastIndexTokenField = nullptr;
        bool lastIsNull = false;
        CreateIndexField(*fieldConfig, lastTokenizeField, lastClassifiedDoc, &lastIndexTokenField, &lastIsNull);
        addModifiedTokens(fieldId, indexTokenField, isNull, lastIndexTokenField, lastIsNull,
                          classifiedDoc->getIndexDocument());
    }
}

void ExtendDocFieldsConvertor::CreateIndexField(const indexlibv2::config::FieldConfig& fieldConfig,
                                                const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
                                                const std::shared_ptr<ClassifiedDocument>& classifiedDoc,
                                                indexlib::document::IndexTokenizeField** indexTokenizeField,
                                                bool* isNull)
{
    fieldid_t fieldId = fieldConfig.GetFieldId();
    *isNull = false;
    *indexTokenizeField = nullptr;
    if (tokenizeField->isNull() and fieldConfig.IsEnableNullField()) {
        indexlib::document::Field* field =
            classifiedDoc->createIndexField(fieldId, indexlib::document::Field::FieldTag::NULL_FIELD);
        (void)field;
        *isNull = true;
    } else {
        if (tokenizeField->isEmpty()) {
            return;
        }
        indexlib::document::Field* field =
            classifiedDoc->createIndexField(fieldId, indexlib::document::Field::FieldTag::TOKEN_FIELD);
        *indexTokenizeField = static_cast<indexlib::document::IndexTokenizeField*>(field);
        transTokenizeFieldToField(tokenizeField, *indexTokenizeField, fieldId, classifiedDoc);
    }
}

void ExtendDocFieldsConvertor::addModifiedTokens(fieldid_t fieldId,
                                                 const indexlib::document::IndexTokenizeField* tokenizeField,
                                                 bool isNull,
                                                 const indexlib::document::IndexTokenizeField* lastTokenizeField,
                                                 bool lastIsNull,
                                                 const std::shared_ptr<indexlib::document::IndexDocument>& indexDoc)
{
    std::vector<uint64_t> terms;
    std::vector<uint64_t> lastTerms;
    auto sortAndUnique = [](const indexlib::document::IndexTokenizeField* tokenizeField, std::vector<uint64_t>* terms) {
        for (auto iter = tokenizeField->Begin(); iter != tokenizeField->End(); ++iter) {
            indexlib::document::Section* section = *iter;
            assert(section);
            for (size_t i = 0; i < section->GetTokenCount(); ++i) {
                indexlib::document::Token* indexToken = section->GetToken(i);
                assert(indexToken);
                terms->push_back(indexToken->GetHashKey());
            }
        }
        std::sort(terms->begin(), terms->end());
        terms->erase(std::unique(terms->begin(), terms->end()), terms->end());
    };
    if (tokenizeField) {
        sortAndUnique(tokenizeField, &terms);
    }
    if (lastTokenizeField) {
        sortAndUnique(lastTokenizeField, &lastTerms);
    }
    std::vector<uint64_t> addTerms;
    std::vector<uint64_t> removeTerms;
    std::set_difference(terms.begin(), terms.end(), lastTerms.begin(), lastTerms.end(), std::back_inserter(addTerms));
    std::set_difference(lastTerms.begin(), lastTerms.end(), terms.begin(), terms.end(),
                        std::back_inserter(removeTerms));
    for (uint64_t term : addTerms) {
        indexDoc->PushModifiedToken(fieldId, term, indexlib::document::ModifiedTokens::Operation::ADD);
    }
    for (uint64_t term : removeTerms) {
        indexDoc->PushModifiedToken(fieldId, term, indexlib::document::ModifiedTokens::Operation::REMOVE);
    }
    indexDoc->SetNullTermModifiedOperation(fieldId, indexlib::document::ModifiedTokens::Operation::NONE);
    if (isNull != lastIsNull) {
        auto op = isNull ? indexlib::document::ModifiedTokens::Operation::ADD
                         : indexlib::document::ModifiedTokens::Operation::REMOVE;
        indexDoc->SetNullTermModifiedOperation(fieldId, op);
    }
}

void ExtendDocFieldsConvertor::convertAttributeField(
    const NormalExtendDocument* document, const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig,
    bool emptyFieldNotEncode)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    if ((size_t)fieldId >= _attrConvertVec.size()) {
        std::stringstream ss;
        ss << "field config error: fieldName[" << fieldConfig->GetFieldName() << "], fieldId[" << fieldId << "]";
        std::string errorMsg = ss.str();
        AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    const auto& convertor = _attrConvertVec[fieldId];
    assert(convertor);
    const std::shared_ptr<ClassifiedDocument>& classifiedDoc = document->getClassifiedDocument();
    const auto& attrDoc = classifiedDoc->getAttributeDoc();
    const autil::StringView& fieldValue = classifiedDoc->getAttributeField(fieldId);
    if (fieldValue.empty()) {
        const auto& rawDoc = document->getRawDocument();
        const autil::StringView& rawField = rawDoc->getField(autil::StringView(fieldConfig->GetFieldName()));
        if (rawField.data() == NULL) {
            if (fieldConfig->IsEnableNullField()) {
                attrDoc->SetNullField(fieldId);
                return;
            }
            AUTIL_LOG(DEBUG, "field [%s] not exist in RawDocument!", fieldConfig->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "field [%s] not exist in RawDocument [%s]!", fieldConfig->GetFieldName().c_str(),
                                rawDoc->toString().c_str());
            IE_RAW_DOC_FORMAT_TRACE(rawDoc, "parse error: field [%s] not exist in RawDocument!",
                                    fieldConfig->GetFieldName().c_str());
            attrDoc->SetFormatError(true);
            if (emptyFieldNotEncode) {
                return;
            }
        }
        if (fieldConfig->IsEnableNullField() &&
            rawField == autil::StringView(fieldConfig->GetNullFieldLiteralString())) {
            attrDoc->SetNullField(fieldId);
            return;
        }
        autil::StringView convertedValue =
            convertor->Encode(rawField, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    } else {
        autil::StringView convertedValue =
            convertor->Encode(fieldValue, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    }
}

void ExtendDocFieldsConvertor::convertSummaryField(const NormalExtendDocument* document,
                                                   const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const std::shared_ptr<ClassifiedDocument>& classifiedDoc = document->getClassifiedDocument();
    const autil::StringView& pluginSetField = classifiedDoc->getSummaryField(fieldId);
    if (!pluginSetField.empty()) {
        return;
    }

    if (fieldConfig->GetFieldType() != ft_text) {
        const std::shared_ptr<RawDocument>& rawDoc = document->getRawDocument();
        const std::string& fieldName = fieldConfig->GetFieldName();
        const autil::StringView& fieldValue = rawDoc->getField(autil::StringView(fieldName));
        // memory is in raw document.
        // and will serialize to indexlib, so do not copy here.
        classifiedDoc->setSummaryFieldNoCopy(fieldId, fieldValue);
    } else {
        const std::shared_ptr<indexlib::document::TokenizeDocument>& tokenizeDoc = document->getTokenizeDocument();
        const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField = tokenizeDoc->getField(fieldId);
        std::string summaryStr = transTokenizeFieldToSummaryStr(tokenizeField, fieldConfig);
        // need copy
        classifiedDoc->setSummaryField(fieldId, summaryStr);
    }
}

std::string ExtendDocFieldsConvertor::transTokenizeFieldToSummaryStr(
    const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
    const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    std::string summaryStr;
    if (!tokenizeField.get()) {
        return fieldConfig->IsEnableNullField() ? fieldConfig->GetNullFieldLiteralString() : summaryStr;
    }

    if (tokenizeField->isNull()) {
        return fieldConfig->GetNullFieldLiteralString();
    }

    indexlib::document::TokenizeField::Iterator it = tokenizeField->createIterator();
    while (!it.isEnd()) {
        if ((*it) == NULL) {
            it.next();
            continue;
        }
        indexlib::document::TokenizeSection::Iterator tokenIter = (*it)->createIterator();
        while (tokenIter) {
            if (!summaryStr.empty()) {
                summaryStr.append("\t");
            }
            const std::string& text = (*tokenIter)->getText();
            summaryStr.append(text.begin(), text.end());
            tokenIter.nextBasic();
        }
        it.next();
    }
    return summaryStr;
}

void ExtendDocFieldsConvertor::transTokenizeFieldToField(
    const std::shared_ptr<indexlib::document::TokenizeField>& tokenizeField,
    indexlib::document::IndexTokenizeField* field, fieldid_t fieldId,
    const std::shared_ptr<ClassifiedDocument>& classifiedDoc)
{
    indexlib::document::TokenizeField::Iterator it = tokenizeField->createIterator();
    if (it.isEnd()) {
        return;
    }
    if (_spatialFieldEncoder->IsSpatialIndexField(fieldId)) {
        addSectionTokens(_spatialFieldEncoder, field, *it, classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    if (_dateFieldEncoder->IsDateIndexField(fieldId)) {
        addSectionTokens(_dateFieldEncoder, field, *it, classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    if (_rangeFieldEncoder->IsRangeIndexField(fieldId)) {
        addSectionTokens(_rangeFieldEncoder, field, *it, classifiedDoc->getPool(), fieldId, classifiedDoc);
        return;
    }
    assert(tokenizeField);
    pos_t lastTokenPos = 0;
    pos_t curPos = -1;
    while (!it.isEnd()) {
        indexlib::document::TokenizeSection* section = *it;
        if (!addSection(field, section, classifiedDoc->getPool(), fieldId, classifiedDoc, lastTokenPos, curPos)) {
            return;
        }
        it.next();
    }
}

bool ExtendDocFieldsConvertor::addSection(indexlib::document::IndexTokenizeField* field,
                                          indexlib::document::TokenizeSection* tokenizeSection,
                                          autil::mem_pool::Pool* pool, fieldid_t fieldId,
                                          const std::shared_ptr<ClassifiedDocument>& classifiedDoc, pos_t& lastTokenPos,
                                          pos_t& curPos)
{
    // TODO: empty section
    uint32_t leftTokenCount = tokenizeSection->getTokenCount();
    indexlib::document::Section* indexSection =
        classifiedDoc->createSection(field, leftTokenCount, tokenizeSection->getSectionWeight());
    if (indexSection == NULL) {
        AUTIL_LOG(DEBUG, "Failed to create new section.");
        return false;
    }
    indexlib::document::IndexDocument::TermOriginValueMap termOriginValueMap;
    indexlib::document::TokenizeSection::Iterator it = tokenizeSection->createIterator();
    section_len_t nowSectionLen = 0;
    section_len_t maxSectionLen = classifiedDoc->getMaxSectionLenght();
    while (*it != NULL) {
        if ((*it)->isSpace() || (*it)->isDelimiter()) {
            it.nextBasic();
            leftTokenCount--;
            continue;
        }

        if (nowSectionLen + 1 >= maxSectionLen) {
            indexSection->SetLength(nowSectionLen + 1);
            nowSectionLen = 0;
            indexSection = classifiedDoc->createSection(field, leftTokenCount, tokenizeSection->getSectionWeight());
            if (indexSection == NULL) {
                AUTIL_LOG(DEBUG, "Failed to create new section.");
                return false;
            }
            curPos++;
        }
        curPos++;
        if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos, termOriginValueMap)) {
            break;
        }

        while (it.nextExtend()) {
            if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos, termOriginValueMap)) {
                break;
            }
        }
        nowSectionLen++;
        leftTokenCount--;
        it.nextBasic();
    }
    indexSection->SetLength(nowSectionLen + 1);
    curPos++;

    if (!termOriginValueMap.empty()) {
        classifiedDoc->getIndexDocument()->AddTermOriginValue(termOriginValueMap);
    }
    return true;
}

void ExtendDocFieldsConvertor::addTermOriginValueMap(
    const std::string& termStr, uint64_t hashKey, fieldid_t fieldId,
    indexlib::document::IndexDocument::TermOriginValueMap& termOriginValueMap) const
{
    for (const auto& [indexName, invertedIndexConfig] : _indexName2InvertedIndexConfig) {
        if (invertedIndexConfig->IsInIndex(fieldId)) {
            termOriginValueMap[indexName].insert(std::make_pair(hashKey, termStr));
        }
    }
}

bool ExtendDocFieldsConvertor::addToken(indexlib::document::Section* indexSection,
                                        const indexlib::document::AnalyzerToken* token, autil::mem_pool::Pool* pool,
                                        fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos,
                                        indexlib::document::IndexDocument::TermOriginValueMap& termOriginValueMap)
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
    bool success = addHashToken(indexSection, hashKey, pool, fieldId, token->getPosPayLoad(), lastTokenPos, curPos);
    if (!success) {
        return success;
    }
    addTermOriginValueMap(text, hashKey, fieldId, termOriginValueMap);
    return true;
}

bool ExtendDocFieldsConvertor::addHashToken(indexlib::document::Section* indexSection, uint64_t hashKey,
                                            autil::mem_pool::Pool* pool, fieldid_t fieldId, pospayload_t posPayload,
                                            pos_t& lastTokenPos, pos_t& curPos)
{
    indexlib::document::Token* indexToken = NULL;
    indexToken = indexSection->CreateToken(hashKey);

    if (!indexToken) {
        curPos--;
        AUTIL_LOG(INFO, "token count overflow in one section, max %u", MAX_TOKEN_PER_SECTION);
        ERROR_COLLECTOR_LOG(ERROR, "token count overflow in one section, max %u", MAX_TOKEN_PER_SECTION);
        return false;
    }
    indexToken->SetPosPayload(posPayload);
    indexToken->SetPosIncrement(curPos - lastTokenPos);
    lastTokenPos = curPos;
    return true;
}

void ExtendDocFieldsConvertor::init()
{
    initAttrConvert();
    initFieldTokenHasherTypeVector();

    // TODO: support spatial index
    _spatialFieldEncoder.reset(
        new indexlib::index::SpatialFieldEncoder(_schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)));
    _dateFieldEncoder.reset(
        new indexlib::index::DateFieldEncoder(_schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)));
    _rangeFieldEncoder.reset(
        new indexlib::index::RangeFieldEncoder(_schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)));

    for (auto& indexConfig : _schema->GetIndexConfigs(index::STATISTICS_TERM_INDEX_TYPE_STR)) {
        auto statConfig = std::dynamic_pointer_cast<index::StatisticsTermIndexConfig>(indexConfig);
        assert(statConfig != nullptr);
        for (const auto& indexName : statConfig->GetInvertedIndexNames()) {
            auto indexConfig = _schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName);
            auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig != nullptr);
            _indexName2InvertedIndexConfig[indexName] = invertedIndexConfig;
        }
    }
}

void ExtendDocFieldsConvertor::initFieldTokenHasherTypeVector()
{
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    _fieldIdToTokenHasher.resize(fieldConfigs.size());
    std::set<fieldid_t> indexFieldIds;
    auto insertFieldIds =
        [&indexFieldIds](const std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>>& fields) {
            for (const auto& field : fields) {
                indexFieldIds.insert(field->GetFieldId());
            }
        };
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR));
    insertFieldIds(_schema->GetIndexFieldConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR));
    if (indexFieldIds.empty()) {
        return;
    }
    for (const auto& fieldConfig : fieldConfigs) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (indexFieldIds.find(fieldId) == indexFieldIds.end()) {
            continue;
        }
        const auto& jsonMap = fieldConfig->GetUserDefinedParam();
        const auto& kvMap = indexlib::util::ConvertFromJsonMap(jsonMap);
        _fieldIdToTokenHasher[fieldId] = indexlib::index::TokenHasher(kvMap, fieldConfig->GetFieldType());
    }
}

void ExtendDocFieldsConvertor::initAttrConvert()
{
    const auto& fieldConfigs = _schema->GetFieldConfigs();
    if (fieldConfigs.empty()) {
        return;
    }
    _attrConvertVec.resize(fieldConfigs.size());

    const auto& attrConfigs = GetAttributeConfigs();
    for (const auto& attrConfig : attrConfigs) {
        const auto fieldConfigPtr = attrConfig->GetFieldConfig();
        _attrConvertVec[fieldConfigPtr->GetFieldId()].reset(
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    }
}
std::vector<std::shared_ptr<indexlibv2::index::AttributeConfig>> ExtendDocFieldsConvertor::GetAttributeConfigs() const
{
    std::vector<std::shared_ptr<indexlibv2::index::AttributeConfig>> result;
    const auto& attrIndexConfigs = _schema->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);
    result.reserve(_schema->GetFieldConfigs().size());
    for (const auto& indexConfig : attrIndexConfigs) {
        const auto& attrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        assert(attrConfig);
        result.push_back(attrConfig);
    }
    const auto& packAttrIndexConfigs = _schema->GetIndexConfigs(indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : packAttrIndexConfigs) {
        const auto& packAttrConfig = std::dynamic_pointer_cast<indexlibv2::index::PackAttributeConfig>(indexConfig);
        assert(packAttrConfig);
        const auto& attrConfigs = packAttrConfig->GetAttributeConfigVec();
        result.insert(result.end(), attrConfigs.begin(), attrConfigs.end());
    }
    return result;
}

template <typename EncoderPtr>
void ExtendDocFieldsConvertor::addSectionTokens(const EncoderPtr& encoder,
                                                indexlib::document::IndexTokenizeField* field,
                                                indexlib::document::TokenizeSection* tokenizeSection,
                                                autil::mem_pool::Pool* pool, fieldid_t fieldId,
                                                const std::shared_ptr<ClassifiedDocument>& classifiedDoc)
{
    uint32_t tokenCount = tokenizeSection->getTokenCount();
    if (tokenCount != 1 && !supportMultiToken(encoder)) {
        ERROR_COLLECTOR_LOG(DEBUG, "field [%s] content illegal, dropped",
                            _schema->GetFieldConfig(fieldId)->GetFieldName().c_str());
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
        indexlib::document::Section* indexSection = classifiedDoc->createSection(field, leftTokenCount, 0);
        if (indexSection == NULL) {
            AUTIL_LOG(DEBUG, "Failed to create new section.");
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
                    AUTIL_LOG(DEBUG, "Failed to create new section.");
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

}} // namespace indexlibv2::document
