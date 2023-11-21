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
#include "sql/ops/join/HashJoinMapR.h"

#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/HashUtil.h"
#include "autil/MultiValueType.h"
#include "sql/common/Log.h"
#include "table/Table.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

static const size_t HASH_VALUE_BUFFER_LIMIT = 100000;

const std::string HashJoinMapR::RESOURCE_ID = "hash_join_map_r";

HashJoinMapR::HashJoinMapR() {}
HashJoinMapR::~HashJoinMapR() {}

void HashJoinMapR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

navi::ErrorCode HashJoinMapR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

void HashJoinMapR::createHashMap(const HashValues &values) {
    _hashJoinMap.clear();
    for (const auto &valuePair : values) {
        const auto &hashKey = valuePair.second;
        const auto &hashValue = valuePair.first;
        auto iter = _hashJoinMap.find(hashKey);
        if (iter == _hashJoinMap.end()) {
            vector<size_t> hashValues = {hashValue};
            _hashJoinMap.insert(make_pair(hashKey, std::move(hashValues)));
        } else {
            iter->second.push_back(hashValue);
        }
    }
}

bool HashJoinMapR::getHashValues(const table::TablePtr &table,
                                 size_t offset,
                                 size_t count,
                                 const vector<string> &joinColumns,
                                 HashValues &values) {
    if (!getColumnHashValues(table, offset, count, joinColumns[0], values)) {
        return false;
    }
    for (size_t i = 1; i < joinColumns.size(); ++i) {
        HashValues tmpValues;
        if (!getColumnHashValues(table, offset, count, joinColumns[i], tmpValues)) {
            return false;
        }
        combineHashValues(tmpValues, values);
    }
    return true;
}

bool HashJoinMapR::getColumnHashValues(const table::TablePtr &table,
                                       size_t offset,
                                       size_t count,
                                       const string &columnName,
                                       HashValues &values) {
    auto joinColumn = table->getColumn(columnName);
    if (joinColumn == nullptr) {
        SQL_LOG(ERROR, "invalid join column name [%s]", columnName.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get join column [%s] schema failed", columnName.c_str());
        return false;
    }
    size_t rowCount = min(offset + count, table->getRowCount());
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, true>::CppType T;                    \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            values.reserve(std::max(rowCount * 4, HASH_VALUE_BUFFER_LIMIT));                       \
            size_t dataSize = 0;                                                                   \
            for (size_t i = offset; i < rowCount; i++) {                                           \
                const auto &datas = columnData->get(i);                                            \
                dataSize = datas.size();                                                           \
                for (size_t k = 0; k < dataSize; k++) {                                            \
                    auto hashKey = HashUtil::calculateHashValue(datas[k]);                         \
                    values.emplace_back(i, hashKey);                                               \
                }                                                                                  \
            }                                                                                      \
        } else {                                                                                   \
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, false>::CppType T;                   \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            values.reserve(rowCount);                                                              \
            for (size_t i = offset; i < rowCount; i++) {                                           \
                const auto &data = columnData->get(i);                                             \
                auto hashKey = HashUtil::calculateHashValue(data);                                 \
                values.emplace_back(i, hashKey);                                                   \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }
    if (values.empty()) {
        _shouldClearTable = true; // for multi-type datas may be empty
        SQL_LOG(WARN,
                "table size[%zu], hash values size[%zu], should clear",
                table->getRowCount(),
                values.size());
    }
    return true;
}

void HashJoinMapR::combineHashValues(const HashValues &valuesA, HashValues &valuesB) {
    HashValues tmpValues;
    size_t beginA = 0;
    size_t beginB = 0;
    while (beginA < valuesA.size() && beginB < valuesB.size()) {
        size_t rowIndexA = valuesA[beginA].first;
        size_t rowIndexB = valuesB[beginB].first;
        if (rowIndexA < rowIndexB) {
            ++beginA;
        } else if (rowIndexA == rowIndexB) {
            size_t offsetB = beginB;
            for (; offsetB < valuesB.size() && rowIndexA == valuesB[offsetB].first; offsetB++) {
                size_t seed = valuesA[beginA].second;
                HashUtil::combineHash(seed, valuesB[offsetB].second);
                tmpValues.emplace_back(rowIndexA, seed);
            }
            ++beginA;
        } else {
            ++beginB;
        }
    }
    valuesB = std::move(tmpValues);
}

REGISTER_RESOURCE(HashJoinMapR);

} // namespace sql
