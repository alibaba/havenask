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
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/util/IndexFields.h"

namespace indexlibv2::index::ann {

class IndexFieldParser
{
public:
    enum ParseStatus { PS_OK, PS_IGNORE, PS_FAIL };

public:
    IndexFieldParser() {}
    ~IndexFieldParser() {}

    bool Init(const std::shared_ptr<indexlibv2::config::ANNIndexConfig>& annIndexConfig);
    ParseStatus Parse(const indexlib::document::IndexDocument* doc, IndexFields& fieldData);

public:
    static bool ConvertToBinaryEmbedding(const std::vector<float>& floatEmb, uint32_t dimension,
                                         embedding_t& binaryEmb);
    template <typename T>
    static bool ParseFromString(const std::string& input, std::vector<T>& output, const std::string& delimiter);

private:
    bool DoParse(const std::vector<const indexlib::document::Field*>& fields, docid_t docId, IndexFields& fieldData);
    bool ParseKey(const indexlib::document::Field* field, int64_t& key);
    bool ParseIndexId(const indexlib::document::Field* field, std::vector<index_id_t>& indexIds);
    bool ParseEmbedding(const indexlib::document::Field* field, embedding_t& embedding);
    bool ParseFromTokenField(const indexlib::document::Field* field, std::vector<float>& vals);
    FieldType GetFieldType(fieldid_t fieldId);

private:
    void TEST_SetFieldType(fieldid_t fieldId, FieldType fieldType);

private:
    AithetaIndexConfig _aithetaConfig {};
    std::vector<fieldid_t> _fieldIds; // field ids related to this indexer
    std::unordered_map<fieldid_t, std::shared_ptr<indexlibv2::config::FieldConfig>> _fieldConfigMap;
    std::string _indexName;

    AUTIL_LOG_DECLARE();
};

template <typename T>
bool IndexFieldParser::ParseFromString(const std::string& input, std::vector<T>& output, const std::string& delimiter)
{
    std::vector<std::string> strs;
    autil::StringUtil::fromString(input, strs, delimiter);
    for (const auto& str : strs) {
        T val = T();
        ANN_CHECK(autil::StringUtil::fromString(str, val), "parse[%s] failed", str.c_str());
        ANN_CHECK(!std::isnan(val) && std::isfinite(val), "val[%s] is invalid", str.c_str());
        output.push_back(val);
    }
    return true;
}

} // namespace indexlibv2::index::ann
