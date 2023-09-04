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
#include "indexlib/index/ann/aitheta2/util/IndexFieldParser.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/normal/IndexRawField.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/util/FloatUint64Encoder.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlibv2::config;

namespace indexlibv2::index::ann {

AUTIL_LOG_SETUP(indexlib.index, IndexFieldParser);

bool IndexFieldParser::Init(const shared_ptr<indexlibv2::config::ANNIndexConfig>& annIndexConfig)
{
    ANN_CHECK(nullptr != annIndexConfig, "ann index config is nullptr");
    _indexName = annIndexConfig->GetIndexName();
    _aithetaConfig = AithetaIndexConfig(annIndexConfig->GetParameters());
    indexlibv2::config::InvertedIndexConfig::Iterator iter = annIndexConfig->CreateIterator();
    while (iter.HasNext()) {
        const auto& fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        _fieldIds.push_back(fieldId);
        _fieldConfigMap[fieldId] = fieldConfig;
    }
    ANN_CHECK(!_fieldIds.empty(), "index [%s] has invalid fields.", _indexName.c_str());
    return true;
}

IndexFieldParser::ParseStatus IndexFieldParser::Parse(const indexlib::document::IndexDocument* doc,
                                                      EmbeddingFieldData& fieldData)
{
    std::vector<const indexlib::document::Field*> fields;
    for (size_t i = 0; i != _fieldIds.size(); ++i) {
        const indexlib::document::Field* field = doc->GetField(_fieldIds[i]);
        if (nullptr == field) {
            if (_aithetaConfig.buildConfig.ignoreFieldCountMismatch) {
                AUTIL_INTERVAL_LOG(1024, WARN, "field [%s] is miss in index [%s]",
                                   _fieldConfigMap[_fieldIds[i]]->GetFieldName().c_str(), _indexName.c_str());
                return IndexFieldParser::ParseStatus::PS_IGNORE;
            } else {
                AUTIL_LOG(WARN, "field [%s] is miss in index [%s]",
                          _fieldConfigMap[_fieldIds[i]]->GetFieldName().c_str(), _indexName.c_str());
                return IndexFieldParser::ParseStatus::PS_FAIL;
            }
        }
        fields.push_back(field);
    }

    if (!DoParse(fields, doc->GetDocId(), fieldData)) {
        return IndexFieldParser::ParseStatus::PS_FAIL;
    }
    return ParseStatus::PS_OK;
}

bool IndexFieldParser::DoParse(const std::vector<const indexlib::document::Field*>& fields, docid_t docId,
                               EmbeddingFieldData& fieldData)
{
    ANN_CHECK(ParseKey(fields[0], fieldData.pk), "parse pk failed in[%s]", _indexName.c_str());
    if (fields.size() == 3) {
        ANN_CHECK(ParseIndexId(fields[1], fieldData.indexIds), "parse index ids failed with pk[%ld] in[%s]",
                  fieldData.pk, _indexName.c_str());
    } else {
        fieldData.indexIds.push_back(kDefaultIndexId);
    }
    ANN_CHECK(ParseEmbedding(fields[fields.size() - 1], fieldData.embedding),
              "parse embedding failed with pk[%ld] in[%s]", fieldData.pk, _indexName.c_str());
    fieldData.docId = docId;
    return true;
}

bool IndexFieldParser::ParseKey(const Field* field, int64_t& pk)
{
    if (field->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        auto indexTokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        ANN_CHECK(indexTokenizeField != nullptr, "cast to IndexTokenizeField failed");

        auto section = *(indexTokenizeField->Begin());
        ANN_CHECK(section->GetLength() >= 1, "parse pk failed");
        pk = section->GetToken(0)->GetHashKey();
    } else {
        auto rawField = dynamic_cast<const IndexRawField*>(field);
        ANN_CHECK(rawField != nullptr, "cast to IndexRawField failed");
        const string str(rawField->GetData().data(), rawField->GetData().size());
        ANN_CHECK(StringUtil::strToInt64(str.data(), pk), "parse[%s] failed", str.c_str());
    }

    AUTIL_LOG(DEBUG, "build pk[%ld] in[%s]", pk, _indexName.c_str());
    return true;
}

bool IndexFieldParser::ParseIndexId(const Field* field, vector<index_id_t>& indexIds)
{
    if (field->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        ANN_CHECK(FieldConfig::IsIntegerType(GetFieldType(field->GetFieldId())),
                  "index id field need builtin integer field");

        auto indexTokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        ANN_CHECK(indexTokenizeField != nullptr, "index id field cast to IndexTokenizeField failed");
        ANN_CHECK(indexTokenizeField->GetSectionCount() > 0, "index id IndexTokenizeField no section");

        assert(indexTokenizeField->GetSectionCount() == 1);
        auto iterField = indexTokenizeField->Begin();
        const Section* section = *iterField;
        ANN_CHECK((section != nullptr) && (section->GetTokenCount() > 0),
                  "index id IndexTokenizeField section no token");

        indexIds.reserve(section->GetTokenCount());
        for (size_t i = 0; i < section->GetTokenCount(); ++i) {
            const Token* token = section->GetToken(i);
            indexIds.push_back(static_cast<index_id_t>(token->GetHashKey()));
        }
    } else {
        auto rawField = dynamic_cast<const IndexRawField*>(field);
        ANN_CHECK(rawField != nullptr, "cast to IndexRawField failed");
        const string str = string(rawField->GetData().data(), rawField->GetData().size());
        ANN_CHECK(ParseFromString(str, indexIds, kIndexIdDelimiter), "parse indexIds[%s] failed", str.c_str());
        AUTIL_LOG(DEBUG, "build index id[%s] in[%s]", str.c_str(), _indexName.c_str());
    }
    return true;
}

bool IndexFieldParser::ParseEmbedding(const Field* field, embedding_t& embedding)
{
    vector<float> vals;
    if (field->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        FieldType fieldType = GetFieldType(field->GetFieldId());
        ANN_CHECK(fieldType == ft_float || fieldType == ft_double,
                  "embedding field need builtin multi float/double field");

        auto indexTokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
        ANN_CHECK(indexTokenizeField != nullptr, "embedding field cast to IndexTokenizeField failed");
        ANN_CHECK(indexTokenizeField->GetSectionCount() > 0, "embedding IndexTokenizeField no section");

        assert(indexTokenizeField->GetSectionCount() == 1);
        auto iterField = indexTokenizeField->Begin();
        const Section* section = *iterField;
        ANN_CHECK((section != nullptr) && (section->GetTokenCount() > 0),
                  "embedding IndexTokenizeField section no token");

        vals.reserve(section->GetTokenCount());
        for (size_t i = 0; i < section->GetTokenCount(); ++i) {
            const Token* token = section->GetToken(i);
            if (fieldType == ft_float) {
                vals.push_back(FloatUint64Encoder::Uint32ToFloat(static_cast<uint32_t>(token->GetHashKey())));
            } else {
                vals.push_back(FloatUint64Encoder::Uint64ToDouble(token->GetHashKey()));
            }
        }
    } else {
        auto rawField = dynamic_cast<const IndexRawField*>(field);
        ANN_CHECK(rawField, "cast to IndexRawField failed");

        const string str = string(rawField->GetData().data(), rawField->GetData().size());
        const string& del = _aithetaConfig.embeddingDelimiter;
        ANN_CHECK(ParseFromString(str, vals, del), "parse embedding[%s] failed", str.c_str());
        AUTIL_LOG(DEBUG, "build embedding[%s] in[%s]", str.c_str(), _indexName.c_str());
    }
    uint32_t dim = _aithetaConfig.dimension;
    ANN_CHECK(vals.size() == dim, "dimension of[%lu] != expected[%u]", vals.size(), dim);

    embedding.reset(new (std::nothrow) float[dim], [](float* p) { delete[] p; });
    if (nullptr == embedding) {
        AUTIL_LOG(ERROR, "alloc size[%lu] in type[float] failed", (size_t)dim);
        return false;
    }
    std::copy_n(vals.begin(), dim, embedding.get());
    return true;
}

FieldType IndexFieldParser::GetFieldType(fieldid_t fieldId)
{
    auto iter = _fieldConfigMap.find(fieldId);
    if (iter == _fieldConfigMap.end()) {
        return ft_unknown;
    } else {
        return iter->second->GetFieldType();
    }
}

void IndexFieldParser::TEST_SetFieldType(fieldid_t fieldId, FieldType fieldType)
{
    auto iter = _fieldConfigMap.find(fieldId);
    if (iter != _fieldConfigMap.end()) {
        iter->second->SetFieldType(fieldType);
    }
}

} // namespace indexlibv2::index::ann