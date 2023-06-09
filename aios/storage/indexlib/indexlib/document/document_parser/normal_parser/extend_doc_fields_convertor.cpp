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
#include "indexlib/document/document_parser/normal_parser/extend_doc_fields_convertor.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/misc/doc_tracer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, ExtendDocFieldsConvertor);

const uint32_t ExtendDocFieldsConvertor::MAX_TOKEN_PER_SECTION = Section::MAX_TOKEN_PER_SECTION;

ExtendDocFieldsConvertor::ExtendDocFieldsConvertor(const IndexPartitionSchemaPtr& schema, regionid_t regionId)
    : _schema(schema)
    , _regionId(regionId)
{
    init();
}

ExtendDocFieldsConvertor::~ExtendDocFieldsConvertor() {}

void ExtendDocFieldsConvertor::convertIndexField(const IndexlibExtendDocumentPtr& document,
                                                 const FieldConfigPtr& fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();

    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        const string& fieldValue = document->getRawDocument()->getField(fieldConfig->GetFieldName());
        if (fieldValue.empty()) {
            return;
        }
        auto indexRawField = dynamic_cast<IndexRawField*>(
            classifiedDoc->createIndexField(fieldConfig->GetFieldId(), Field::FieldTag::RAW_FIELD));
        assert(indexRawField);
        indexRawField->SetData(autil::MakeCString(fieldValue, classifiedDoc->getPool()));
        return;
    }
    const TokenizeDocumentPtr& tokenizeDoc = document->getTokenizeDocument();
    const TokenizeDocumentPtr& lastTokenizeDoc = document->getLastTokenizeDocument();
    const TokenizeFieldPtr& tokenizeField = tokenizeDoc->getField(fieldId);
    const TokenizeFieldPtr& lastTokenizeField = lastTokenizeDoc->getField(fieldId);
    if (!tokenizeField) {
        return;
    }

    IndexTokenizeField* indexTokenField = nullptr;
    bool isNull = false;
    CreateIndexField(*fieldConfig, tokenizeField, classifiedDoc, &indexTokenField, &isNull);

    if (lastTokenizeField) {
        ClassifiedDocumentPtr lastClassifiedDoc(new ClassifiedDocument());
        IndexTokenizeField* lastIndexTokenField = nullptr;
        bool lastIsNull = false;
        CreateIndexField(*fieldConfig, lastTokenizeField, lastClassifiedDoc, &lastIndexTokenField, &lastIsNull);
        addModifiedTokens(fieldId, indexTokenField, isNull, lastIndexTokenField, lastIsNull,
                          classifiedDoc->getIndexDocument());
    }
}

void ExtendDocFieldsConvertor::CreateIndexField(const config::FieldConfig& fieldConfig,
                                                const TokenizeFieldPtr& tokenizeField,
                                                const ClassifiedDocumentPtr& classifiedDoc,
                                                IndexTokenizeField** indexTokenizeField, bool* isNull)
{
    fieldid_t fieldId = fieldConfig.GetFieldId();
    *isNull = false;
    *indexTokenizeField = nullptr;
    if (tokenizeField->isNull() and fieldConfig.IsEnableNullField()) {
        Field* field = classifiedDoc->createIndexField(fieldId, Field::FieldTag::NULL_FIELD);
        (void)field;
        *isNull = true;
    } else {
        if (tokenizeField->isEmpty()) {
            return;
        }
        Field* field = classifiedDoc->createIndexField(fieldId, Field::FieldTag::TOKEN_FIELD);
        *indexTokenizeField = static_cast<IndexTokenizeField*>(field);
        transTokenizeFieldToField(tokenizeField, *indexTokenizeField, fieldId, classifiedDoc);
    }
}

void ExtendDocFieldsConvertor::addModifiedTokens(fieldid_t fieldId, const IndexTokenizeField* tokenizeField,
                                                 bool isNull, const IndexTokenizeField* lastTokenizeField,
                                                 bool lastIsNull, const IndexDocumentPtr& indexDoc)
{
    std::vector<uint64_t> terms;
    std::vector<uint64_t> lastTerms;
    auto sortAndUnique = [](const IndexTokenizeField* tokenizeField, std::vector<uint64_t>* terms) {
        for (auto iter = tokenizeField->Begin(); iter != tokenizeField->End(); ++iter) {
            Section* section = *iter;
            assert(section);
            for (size_t i = 0; i < section->GetTokenCount(); ++i) {
                Token* indexToken = section->GetToken(i);
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
        indexDoc->PushModifiedToken(fieldId, term, ModifiedTokens::Operation::ADD);
    }
    for (uint64_t term : removeTerms) {
        indexDoc->PushModifiedToken(fieldId, term, ModifiedTokens::Operation::REMOVE);
    }
    indexDoc->SetNullTermModifiedOperation(fieldId, ModifiedTokens::Operation::NONE);
    if (isNull != lastIsNull) {
        auto op = isNull ? ModifiedTokens::Operation::ADD : ModifiedTokens::Operation::REMOVE;
        indexDoc->SetNullTermModifiedOperation(fieldId, op);
    }
}

void ExtendDocFieldsConvertor::convertAttributeField(const IndexlibExtendDocumentPtr& document,
                                                     const FieldConfigPtr& fieldConfig, bool emptyFieldNotEncode)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    if ((size_t)fieldId >= _attrConvertVec.size()) {
        stringstream ss;
        ss << "field config error: fieldName[" << fieldConfig->GetFieldName() << "], fieldId[" << fieldId << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    const AttributeConvertorPtr& convertor = _attrConvertVec[fieldId];
    assert(convertor);
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    const AttributeDocumentPtr& attrDoc = classifiedDoc->getAttributeDoc();
    const StringView& fieldValue = classifiedDoc->getAttributeField(fieldId);
    if (fieldValue.empty()) {
        const RawDocumentPtr& rawDoc = document->getRawDocument();
        const StringView& rawField = rawDoc->getField(StringView(fieldConfig->GetFieldName()));
        if (rawField.data() == NULL) {
            if (fieldConfig->IsEnableNullField()) {
                attrDoc->SetNullField(fieldId);
                return;
            }
            IE_LOG(DEBUG, "field [%s] not exist in RawDocument!", fieldConfig->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "field [%s] not exist in RawDocument [%s]!", fieldConfig->GetFieldName().c_str(),
                                rawDoc->toString().c_str());
            IE_RAW_DOC_FORMAT_TRACE(rawDoc, "parse error: field [%s] not exist in RawDocument!",
                                    fieldConfig->GetFieldName().c_str());
            attrDoc->SetFormatError(true);
            if (emptyFieldNotEncode) {
                return;
            }
        }
        if (fieldConfig->IsEnableNullField() && rawField == StringView(fieldConfig->GetNullFieldLiteralString())) {
            attrDoc->SetNullField(fieldId);
            return;
        }
        StringView convertedValue =
            convertor->Encode(rawField, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    } else {
        StringView convertedValue =
            convertor->Encode(fieldValue, classifiedDoc->getPool(), attrDoc->GetFormatErrorLable());
        classifiedDoc->setAttributeFieldNoCopy(fieldId, convertedValue);
    }
}

void ExtendDocFieldsConvertor::convertSummaryField(const IndexlibExtendDocumentPtr& document,
                                                   const FieldConfigPtr& fieldConfig)
{
    fieldid_t fieldId = fieldConfig->GetFieldId();
    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    const StringView& pluginSetField = classifiedDoc->getSummaryField(fieldId);
    if (!pluginSetField.empty()) {
        return;
    }

    if (fieldConfig->GetFieldType() != ft_text) {
        const RawDocumentPtr& rawDoc = document->getRawDocument();
        const string& fieldName = fieldConfig->GetFieldName();
        const StringView& fieldValue = rawDoc->getField(StringView(fieldName));
        // memory is in raw document.
        // and will serialize to indexlib, so do not copy here.
        classifiedDoc->setSummaryFieldNoCopy(fieldId, fieldValue);
    } else {
        const TokenizeDocumentPtr& tokenizeDoc = document->getTokenizeDocument();
        const TokenizeFieldPtr& tokenizeField = tokenizeDoc->getField(fieldId);
        string summaryStr = transTokenizeFieldToSummaryStr(tokenizeField, fieldConfig);
        // need copy
        classifiedDoc->setSummaryField(fieldId, summaryStr);
    }
}

string ExtendDocFieldsConvertor::transTokenizeFieldToSummaryStr(const TokenizeFieldPtr& tokenizeField,
                                                                const FieldConfigPtr& fieldConfig)
{
    string summaryStr;
    if (!tokenizeField.get()) {
        return fieldConfig->IsEnableNullField() ? fieldConfig->GetNullFieldLiteralString() : summaryStr;
    }

    if (tokenizeField->isNull()) {
        return fieldConfig->GetNullFieldLiteralString();
    }

    TokenizeField::Iterator it = tokenizeField->createIterator();
    while (!it.isEnd()) {
        if ((*it) == NULL) {
            it.next();
            continue;
        }
        TokenizeSection::Iterator tokenIter = (*it)->createIterator();
        while (tokenIter) {
            if (!summaryStr.empty()) {
                summaryStr.append("\t");
            }
            const string& text = (*tokenIter)->getText();
            summaryStr.append(text.begin(), text.end());
            tokenIter.nextBasic();
        }
        it.next();
    }
    return summaryStr;
}

void ExtendDocFieldsConvertor::transTokenizeFieldToField(const TokenizeFieldPtr& tokenizeField,
                                                         IndexTokenizeField* field, fieldid_t fieldId,
                                                         const ClassifiedDocumentPtr& classifiedDoc)
{
    TokenizeField::Iterator it = tokenizeField->createIterator();
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
        TokenizeSection* section = *it;
        if (!addSection(field, section, classifiedDoc->getPool(), fieldId, classifiedDoc, lastTokenPos, curPos)) {
            return;
        }
        it.next();
    }
}

bool ExtendDocFieldsConvertor::addSection(IndexTokenizeField* field, TokenizeSection* tokenizeSection, Pool* pool,
                                          fieldid_t fieldId, const ClassifiedDocumentPtr& classifiedDoc,
                                          pos_t& lastTokenPos, pos_t& curPos)
{
    // TODO: empty section
    uint32_t leftTokenCount = tokenizeSection->getTokenCount();
    Section* indexSection = classifiedDoc->createSection(field, leftTokenCount, tokenizeSection->getSectionWeight());
    if (indexSection == NULL) {
        IE_LOG(DEBUG, "Failed to create new section.");
        return false;
    }
    TokenizeSection::Iterator it = tokenizeSection->createIterator();
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
                IE_LOG(DEBUG, "Failed to create new section.");
                return false;
            }
            curPos++;
        }
        curPos++;
        if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
            break;
        }
        while (it.nextExtend()) {
            if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
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

bool ExtendDocFieldsConvertor::addToken(Section* indexSection, const AnalyzerToken* token, Pool* pool,
                                        fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos)
{
    if (token->isSpace() || token->isStopWord()) {
        // do nothing
        return true;
    }

    const string& text = token->getNormalizedText();
    if (_customizedIndexFieldEncoder->IsFloatField(fieldId)) {
        std::vector<dictkey_t> dictKeys;
        _customizedIndexFieldEncoder->Encode(fieldId, text, dictKeys);
        for_each(dictKeys.begin(), dictKeys.end(), [&](dictkey_t key) {
            addHashToken(indexSection, key, pool, fieldId, token->getPosPayLoad(), lastTokenPos, curPos);
        });
        return true;
    }

    const index::TokenHasher& tokenHasher = _fieldIdToTokenHasher[fieldId];
    uint64_t hashKey;
    if (!tokenHasher.CalcHashKey(text, hashKey)) {
        return true;
    }
    return addHashToken(indexSection, hashKey, pool, fieldId, token->getPosPayLoad(), lastTokenPos, curPos);
}

bool ExtendDocFieldsConvertor::addHashToken(Section* indexSection, uint64_t hashKey, Pool* pool, fieldid_t fieldId,
                                            pospayload_t posPayload, pos_t& lastTokenPos, pos_t& curPos)
{
    Token* indexToken = NULL;
    indexToken = indexSection->CreateToken(hashKey);

    if (!indexToken) {
        curPos--;
        IE_LOG(INFO, "token count overflow in one section, max %u", MAX_TOKEN_PER_SECTION);
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

    // TODO: _spatialFieldEncoder supports multi region
    auto indexConfigIter = _schema->GetIndexSchema()->CreateIterator();
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> indexConfigs(indexConfigIter->Begin(),
                                                                                indexConfigIter->End());
    _spatialFieldEncoder.reset(new index::SpatialFieldEncoder());
    _spatialFieldEncoder->Init<config::SpatialIndexConfig>(indexConfigs);
    _dateFieldEncoder.reset(new DateFieldEncoder(_schema));
    _rangeFieldEncoder.reset(new RangeFieldEncoder(_schema->GetIndexSchema()));
    _customizedIndexFieldEncoder.reset(new CustomizedIndexFieldEncoder(_schema));
}

void ExtendDocFieldsConvertor::initFieldTokenHasherTypeVector()
{
    const FieldSchemaPtr& fieldSchemaPtr = _schema->GetFieldSchema(_regionId);
    const IndexSchemaPtr& indexSchemaPtr = _schema->GetIndexSchema(_regionId);
    if (!indexSchemaPtr) {
        return;
    }
    _fieldIdToTokenHasher.resize(fieldSchemaPtr->GetFieldCount());
    for (FieldSchema::Iterator it = fieldSchemaPtr->Begin(); it != fieldSchemaPtr->End(); ++it) {
        const FieldConfigPtr& fieldConfig = *it;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!indexSchemaPtr->IsInIndex(fieldId)) {
            continue;
        }
        const auto& jsonMap = fieldConfig->GetUserDefinedParam();
        const auto& kvMap = ConvertFromJsonMap(jsonMap);
        _fieldIdToTokenHasher[fieldId] = index::TokenHasher(kvMap, fieldConfig->GetFieldType());
    }
}

void ExtendDocFieldsConvertor::initAttrConvert()
{
    const FieldSchemaPtr& fieldSchemaPtr = _schema->GetFieldSchema(_regionId);
    if (!fieldSchemaPtr) {
        return;
    }
    _attrConvertVec.resize(fieldSchemaPtr->GetFieldCount());
    const AttributeSchemaPtr& attrSchemaPtr = _schema->GetAttributeSchema(_regionId);
    if (!attrSchemaPtr) {
        return;
    }

    TableType tableType = _schema->GetTableType();
    for (AttributeSchema::Iterator it = attrSchemaPtr->Begin(); it != attrSchemaPtr->End(); ++it) {
        const FieldConfigPtr& fieldConfigPtr = (*it)->GetFieldConfig();
        _attrConvertVec[fieldConfigPtr->GetFieldId()].reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(*it, tableType));
    }
}
}} // namespace indexlib::document
