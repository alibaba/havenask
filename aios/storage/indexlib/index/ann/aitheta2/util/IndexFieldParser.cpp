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
#include "indexlib/index/ann/aitheta2/util/EmbeddingUtil.h"
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
                                                      IndexFields& fieldData)
{
    std::vector<const indexlib::document::Field*> fields;
    for (size_t i = 0; i != _fieldIds.size(); ++i) {
        const indexlib::document::Field* field = doc->GetField(_fieldIds[i]);
        if (field != nullptr) {
            fields.push_back(field);
            continue;
        }
        if (_aithetaConfig.buildConfig.ignoreFieldCountMismatch || _aithetaConfig.buildConfig.ignoreInvalidDoc) {
            AUTIL_INTERVAL_LOG(1024, WARN, "field[%s] is miss in index[%s]",
                               _fieldConfigMap[_fieldIds[i]]->GetFieldName().c_str(), _indexName.c_str());
            return IndexFieldParser::ParseStatus::PS_IGNORE;
        }
        AUTIL_LOG(ERROR, "missing field[%s] in index[%s]", _fieldConfigMap[_fieldIds[i]]->GetFieldName().c_str(),
                  _indexName.c_str());
        return IndexFieldParser::ParseStatus::PS_FAIL;
    }

    if (!DoParse(fields, doc->GetDocId(), fieldData)) {
        if (_aithetaConfig.buildConfig.ignoreInvalidDoc) {
            AUTIL_LOG(DEBUG, "fields parse failed, ignore");
            return IndexFieldParser::ParseStatus::PS_IGNORE;
        }
        return IndexFieldParser::ParseStatus::PS_FAIL;
    }
    return ParseStatus::PS_OK;
}

bool IndexFieldParser::DoParse(const std::vector<const indexlib::document::Field*>& fields, docid_t docId,
                               IndexFields& fieldData)
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
        FieldType fieldType = GetFieldType(field->GetFieldId());
        ANN_CHECK(FieldConfig::IsIntegerType(fieldType) || fieldType == ft_string,
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
    vector<float> floatEmb;
    if (field->GetFieldTag() == Field::FieldTag::TOKEN_FIELD) {
        ANN_CHECK(ParseFromTokenField(field, floatEmb), "parse embedding from token field failed");
    } else {
        auto rawField = dynamic_cast<const IndexRawField*>(field);
        ANN_CHECK(rawField, "cast to IndexRawField failed");
        const string str = string(rawField->GetData().data(), rawField->GetData().size());
        const string& del = _aithetaConfig.embeddingDelimiter;
        ANN_CHECK(ParseFromString(str, floatEmb, del), "parse embedding[%s] failed", str.c_str());
        AUTIL_LOG(DEBUG, "build embedding[%s] in[%s]", str.c_str(), _indexName.c_str());
    }
    ANN_CHECK(floatEmb.size() == _aithetaConfig.dimension, "dimension[%lu] mismatch expected[%u]", floatEmb.size(),
              _aithetaConfig.dimension);

    if (_aithetaConfig.distanceType == HAMMING) {
        return ConvertToBinaryEmbedding(floatEmb, _aithetaConfig.dimension, embedding);
    }

    size_t elemSize = floatEmb.size() * sizeof(float);
    embedding.reset(new (std::nothrow) char[elemSize], std::default_delete<char[]>());
    ANN_CHECK(embedding, "alloc size[%lu]  failed", elemSize);
    memcpy(embedding.get(), floatEmb.data(), elemSize);
    return true;
}

bool IndexFieldParser::ConvertToBinaryEmbedding(const std::vector<float>& floatEmb, uint32_t dimension,
                                                embedding_t& embedding)
{
    vector<int32_t> binaryEmb;
    auto status = EmbeddingUtil::ConvertFloatToBinary(floatEmb.data(), dimension, binaryEmb);
    ANN_CHECK(status.IsOK(), "convert failed, %s", status.ToString().c_str());

    size_t elemSize = binaryEmb.size() * sizeof(int32_t);
    embedding.reset(new (std::nothrow) char[elemSize], std::default_delete<char[]>());
    ANN_CHECK(embedding, "alloc size[%lu] failed", elemSize);
    memcpy(embedding.get(), binaryEmb.data(), elemSize);
    return true;
}

bool IndexFieldParser::ParseFromTokenField(const Field* field, vector<float>& vals)
{
    FieldType fieldType = GetFieldType(field->GetFieldId());
    ANN_CHECK(fieldType == ft_float || fieldType == ft_double, "embedding field need builtin multi float/double field");

    auto indexTokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    ANN_CHECK(indexTokenizeField != nullptr, "embedding field cast to IndexTokenizeField failed");
    ANN_CHECK(indexTokenizeField->GetSectionCount() > 0, "embedding IndexTokenizeField no section");

    size_t tokenCount = 0;
    for (auto iterField = indexTokenizeField->Begin(); iterField != indexTokenizeField->End(); ++iterField) {
        const Section* section = *iterField;
        if (section) {
            tokenCount += section->GetTokenCount();
        }
    }
    ANN_CHECK(tokenCount > 0, "embedding IndexTokenizeField no token");

    vals.reserve(tokenCount);
    for (auto iterField = indexTokenizeField->Begin(); iterField != indexTokenizeField->End(); ++iterField) {
        const Section* section = *iterField;
        if (!section) {
            continue;
        }

        for (size_t i = 0; i < section->GetTokenCount(); ++i) {
            const Token* token = section->GetToken(i);
            float val = 0.0f;
            if (fieldType == ft_float) {
                val = FloatUint64Encoder::Uint32ToFloat(static_cast<uint32_t>(token->GetHashKey()));
            } else if (fieldType == ft_double) {
                val = (float)FloatUint64Encoder::Uint64ToDouble(token->GetHashKey());
            } else {
                AUTIL_LOG(ERROR, "no support for field type[%d]", fieldType);
                return false;
            }
            ANN_CHECK(!std::isnan(val) && std::isfinite(val), "val[%f] is invalid", val);
            vals.push_back(val);
        }
    }
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
