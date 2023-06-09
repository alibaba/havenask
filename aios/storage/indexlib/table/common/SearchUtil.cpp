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
#include "indexlib/table/common/SearchUtil.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/proto/query.pb.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, SearchUtil);

void SearchUtil::ConvertResponseToStringFormat(const base::PartitionResponse& partitionResponse, std::string& result)
{
    const bool hasDocId = partitionResponse.rows_size() > 0 ? partitionResponse.rows(0).has_docid() : false;
    const auto& attrInfo = partitionResponse.attrinfo();

    std::vector<std::map<std::string, std::string>> matchDocs;
    matchDocs.resize(partitionResponse.rows_size());
    for (int64_t i = 0; i < partitionResponse.rows_size(); ++i) {
        const auto& row = partitionResponse.rows(i);
        auto& valueMap = matchDocs[i];
        if (hasDocId) {
            valueMap["__docid__"] = autil::StringUtil::toString(row.docid());
        }
        for (int j = 0; j < attrInfo.fields_size(); ++j) {
            const auto& attrValue = row.attrvalues(j);
            const bool isSingle = attrValue.has_int32_value() || attrValue.has_uint32_value() ||
                                  attrValue.has_int64_value() || attrValue.has_uint64_value() ||
                                  attrValue.has_float_value() || attrValue.has_double_value() ||
                                  attrValue.has_bytes_value();
            std::string valueStr;

#define GET_VALUE_FROM_ATTRVALUE(INDEXLIB_SINGLE_FIELD, INDEXLIB_MULTI_FIELD)                                          \
    if (isSingle) {                                                                                                    \
        valueStr = autil::StringUtil::toString(attrValue.INDEXLIB_SINGLE_FIELD());                                     \
    } else {                                                                                                           \
        for (int i = 0; i < attrValue.INDEXLIB_MULTI_FIELD().value_size(); ++i) {                                      \
            valueStr += autil::StringUtil::toString(attrValue.INDEXLIB_MULTI_FIELD().value(i));                        \
            if (i != attrValue.INDEXLIB_MULTI_FIELD().value_size() - 1) {                                              \
                valueStr += "";                                                                                       \
            }                                                                                                          \
        }                                                                                                              \
    }                                                                                                                  \
    break;

            switch (attrValue.type()) {
            case indexlibv2::base::INT_8:
            case indexlibv2::base::INT_16:
            case indexlibv2::base::INT_32:
                GET_VALUE_FROM_ATTRVALUE(int32_value, multi_int32_value);
            case indexlibv2::base::UINT_8:
            case indexlibv2::base::UINT_16:
            case indexlibv2::base::UINT_32:
                GET_VALUE_FROM_ATTRVALUE(uint32_value, multi_uint32_value);
            case indexlibv2::base::INT_64:
                GET_VALUE_FROM_ATTRVALUE(int64_value, multi_int64_value);
            case indexlibv2::base::UINT_64:
                GET_VALUE_FROM_ATTRVALUE(uint64_value, multi_uint64_value);
            case indexlibv2::base::FLOAT:
                GET_VALUE_FROM_ATTRVALUE(float_value, multi_float_value);
            case indexlibv2::base::DOUBLE:
                GET_VALUE_FROM_ATTRVALUE(double_value, multi_double_value);
            case indexlibv2::base::STRING: {
                if (isSingle) {
                    valueStr = attrValue.bytes_value();
                } else {
                    for (int i = 0; i < attrValue.multi_bytes_value().value_size(); ++i) {
                        valueStr += attrValue.multi_bytes_value().value(i);
                        if (i != attrValue.multi_bytes_value().value_size() - 1) {
                            valueStr += "";
                        }
                    }
                }
                break;
            }
            case indexlibv2::base::INT_128:
            default:;
            }

#undef GET_VALUE_FROM_ATTRVALUE
            valueMap[attrInfo.fields(j).attrname()] = valueStr;
        }
    }

    std::map<std::string, uint32_t> indexTermMetaMap;
    if (partitionResponse.has_termmeta()) {
        indexTermMetaMap["doc_freq"] = partitionResponse.termmeta().docfreq();
        indexTermMetaMap["total_term_frequence"] = partitionResponse.termmeta().totaltermfreq();
        indexTermMetaMap["payload"] = partitionResponse.termmeta().payload();
    }

    std::vector<float> matchValues;
    for (int i = 0; i < partitionResponse.matchvalues_size(); ++i) {
        matchValues.push_back(partitionResponse.matchvalues(i));
    }

    autil::legacy::json::JsonMap jsonMap;
    jsonMap["match_count"] = autil::legacy::ToJson(partitionResponse.rows_size());
    if (!matchDocs.empty()) {
        jsonMap["match_docs"] = autil::legacy::ToJson(matchDocs);
    }
    if (!indexTermMetaMap.empty()) {
        jsonMap["index_term_meta"] = autil::legacy::ToJson(indexTermMetaMap);
    }
    if (!matchValues.empty()) {
        jsonMap["match_values"] = autil::legacy::ToJson(matchValues);
    }
    result = autil::legacy::json::ToString(jsonMap);
}

} // namespace indexlibv2::table
