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
#include "indexlib/document/index_field_convertor.h"

#include "indexlib/common/field_format/customized_index/customized_index_field_encoder.h"
#include "indexlib/common/field_format/date/date_field_encoder.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/common/field_format/spatial/SpatialFieldEncoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, IndexFieldConvertor);

IndexFieldConvertor::IndexFieldConvertor(const IndexPartitionSchemaPtr& schema, regionid_t regionId)
    : mSchema(schema)
    , mRegionId(regionId)
{
    init();
}

IndexFieldConvertor::~IndexFieldConvertor() {}

void IndexFieldConvertor::init()
{
    initFieldTokenHasherTypeVector();
    mSpatialFieldEncoder.reset(new index::SpatialFieldEncoder());
    auto indexConfigIter = mSchema->GetIndexSchema()->CreateIterator();
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> indexConfigs(indexConfigIter->Begin(),
                                                                                indexConfigIter->End());
    mSpatialFieldEncoder->Init<config::SpatialIndexConfig>(indexConfigs);
    mDateFieldEncoder.reset(new DateFieldEncoder(mSchema));
    mRangeFieldEncoder.reset(new RangeFieldEncoder(mSchema->GetIndexSchema()));
    mCustomizedIndexFieldEncoder.reset(new CustomizedIndexFieldEncoder(mSchema));
}

void IndexFieldConvertor::convertIndexField(const IndexDocumentPtr& indexDoc, const FieldConfigPtr& fieldConfig,
                                            const string& fieldValue, const string& fieldSep, Pool* pool)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (fieldType == ft_raw) {
        auto rawField =
            dynamic_cast<IndexRawField*>(indexDoc->CreateField(fieldConfig->GetFieldId(), Field::FieldTag::RAW_FIELD));
        assert(rawField);
        rawField->SetData(autil::MakeCString(fieldValue, pool));
        return;
    }

    if (fieldConfig->IsEnableNullField() && fieldValue == fieldConfig->GetNullFieldLiteralString()) {
        indexDoc->CreateField(fieldConfig->GetFieldId(), Field::FieldTag::NULL_FIELD);
        return;
    }

    Field* field = indexDoc->CreateField(fieldConfig->GetFieldId(), Field::FieldTag::TOKEN_FIELD);
    auto tokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    pos_t lastTokenPos = 0;
    pos_t curPos = -1;

    fieldid_t fieldId = fieldConfig->GetFieldId();
    if (mSpatialFieldEncoder->IsSpatialIndexField(fieldId)) {
        addSpatialSection(fieldId, fieldValue, tokenizeField);
        return;
    }

    if (mDateFieldEncoder->IsDateIndexField(fieldId)) {
        addDateSection(fieldId, fieldValue, tokenizeField);
        return;
    }

    if (mRangeFieldEncoder->IsRangeIndexField(fieldId)) {
        addRangeSection(fieldId, fieldValue, tokenizeField);
        return;
    }
    if (mCustomizedIndexFieldEncoder->IsFloatField(fieldId)) {
        addFloatSection(fieldId, fieldValue, fieldSep, tokenizeField);
        return;
    }
    addSection(tokenizeField, fieldValue, fieldSep, pool, fieldConfig->GetFieldId(), lastTokenPos, curPos);
}

bool IndexFieldConvertor::addSection(IndexTokenizeField* field, const string& fieldValue, const string& fieldSep,
                                     Pool* pool, fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos)
{
    TokenizeSectionPtr tokenizeSection = ParseSection(fieldValue, fieldSep);
    Section* indexSection = field->CreateSection(tokenizeSection->GetTokenCount());
    if (indexSection == NULL) {
        IE_LOG(DEBUG, "Failed to create new section.");
        return false;
    }
    TokenizeSection::Iterator it = tokenizeSection->Begin();
    for (; it != tokenizeSection->End(); ++it) {
        curPos++;
        if (!addToken(indexSection, *it, pool, fieldId, lastTokenPos, curPos)) {
            break;
        }
    }
    indexSection->SetLength(tokenizeSection->GetTokenCount());
    curPos++;
    return true;
}

void IndexFieldConvertor::addSection(const vector<dictkey_t>& dictKeys, IndexTokenizeField* field)
{
    Section* indexSection = field->CreateSection(dictKeys.size());
    if (indexSection == NULL) {
        IE_LOG(DEBUG, "Failed to create new section.");
        return;
    }
    for (size_t i = 0; i < dictKeys.size(); i++) {
        pos_t lastPos = 0;
        pos_t curPos = 0;
        addHashToken(indexSection, dictKeys[i], lastPos, curPos);
    }
}

void IndexFieldConvertor::addDateSection(fieldid_t fieldId, const string& fieldValue, IndexTokenizeField* field)
{
    vector<dictkey_t> dictKeys;
    mDateFieldEncoder->Encode(fieldId, fieldValue, dictKeys);
    addSection(dictKeys, field);
}

void IndexFieldConvertor::addRangeSection(fieldid_t fieldId, const string& fieldValue, IndexTokenizeField* field)
{
    vector<dictkey_t> dictKeys;
    mRangeFieldEncoder->Encode(fieldId, fieldValue, dictKeys);
    addSection(dictKeys, field);
}

void IndexFieldConvertor::addSpatialSection(fieldid_t fieldId, const string& fieldValue, IndexTokenizeField* field)
{
    vector<dictkey_t> dictKeys;
    mSpatialFieldEncoder->Encode(fieldId, fieldValue, dictKeys);
    addSection(dictKeys, field);
}

void IndexFieldConvertor::addFloatSection(fieldid_t fieldId, const string& fieldValue, const string& fieldSep,
                                          IndexTokenizeField* field)
{
    TokenizeSection::TokenVector tokenVec = StringUtil::split(fieldValue, fieldSep);
    if (tokenVec.empty()) {
        return;
    }
    vector<dictkey_t> dictKeys;
    dictKeys.reserve(tokenVec.size());
    for (auto& token : tokenVec) {
        vector<dictkey_t> keys;
        mCustomizedIndexFieldEncoder->Encode(fieldId, token, dictKeys);
        dictKeys.insert(dictKeys.end(), keys.begin(), keys.end());
    }
    addSection(dictKeys, field);
}

bool IndexFieldConvertor::addToken(Section* indexSection, const std::string& token, Pool* pool, fieldid_t fieldId,
                                   pos_t& lastTokenPos, pos_t& curPos)
{
    const index::TokenHasher& tokenHasher = mFieldIdToTokenHasher[fieldId];
    uint64_t hashKey;
    if (!tokenHasher.CalcHashKey(token, hashKey)) {
        return true;
    }
    return addHashToken(indexSection, hashKey, lastTokenPos, curPos);
}

bool IndexFieldConvertor::addHashToken(Section* indexSection, dictkey_t hashKey, pos_t& lastTokenPos, pos_t& curPos)
{
    Token* indexToken = indexSection->CreateToken(hashKey);
    if (!indexToken) {
        curPos--;
        IE_LOG(INFO, "token count overflow in one section");
        return false;
    }
    indexToken->SetPosIncrement(curPos - lastTokenPos);
    lastTokenPos = curPos;
    return true;
}

void IndexFieldConvertor::initFieldTokenHasherTypeVector()
{
    const FieldSchemaPtr& fieldSchemaPtr = mSchema->GetFieldSchema(mRegionId);
    const IndexSchemaPtr& indexSchemaPtr = mSchema->GetIndexSchema(mRegionId);
    if (!indexSchemaPtr) {
        return;
    }
    mFieldIdToTokenHasher.resize(fieldSchemaPtr->GetFieldCount());
    for (FieldSchema::Iterator it = fieldSchemaPtr->Begin(); it != fieldSchemaPtr->End(); ++it) {
        const FieldConfigPtr& fieldConfig = *it;
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (!indexSchemaPtr->IsInIndex(fieldId)) {
            continue;
        }
        const auto& jsonMap = fieldConfig->GetUserDefinedParam();
        const auto& kvMap = ConvertFromJsonMap(jsonMap);
        mFieldIdToTokenHasher[fieldId] = index::TokenHasher(kvMap, fieldConfig->GetFieldType());
    }
}

IndexFieldConvertor::TokenizeSectionPtr IndexFieldConvertor::ParseSection(const string& sectionStr, const string& sep)
{
    TokenizeSection::TokenVector tokenVec = StringUtil::split(sectionStr, sep);
    TokenizeSectionPtr tokenizeSection(new TokenizeSection);
    tokenizeSection->SetTokenVector(tokenVec);
    return tokenizeSection;
}
}} // namespace indexlib::document
