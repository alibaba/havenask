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
#include "ha3/sql/ops/tvf/builtin/RerankBase.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <queue>
#include <unordered_map>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/Pool.h"
#include "autil/MultiValueCreator.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "autil/HashUtil.h"
#include "matchdoc/ValueType.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/ValueTypeSwitch.h"
#include "table/TableUtil.h"

namespace isearch {
namespace sql {
bool RerankBase::init(TvfFuncInitContext &context) {
    (void) context;
    return true;
}

bool RerankBase::doCompute(const table::TablePtr &input, table::TablePtr &output) {
    if (computeTable(input)) {
        output = input;
        return true;
    } else {
        return false;
    }
}

bool RerankBase::computeTable(const table::TablePtr &input) {
    (void) input;
    return true;
}

bool RerankBase::checkQuotaField(const table::TablePtr& table) {
    if (table == nullptr) {
        SQL_LOG(ERROR, "Table is null");
        return false;
    }
    for (const std::string& field : _quotaFields) {
        auto column = table->getColumn(field);
        if (column == nullptr) {
            SQL_LOG(ERROR, "Could not find column [%s] in table", field.c_str());
            return false;
        }
        if (column->getColumnSchema() == nullptr) {
            SQL_LOG(ERROR, "Could not find column [%s] schema", field.c_str());
            return false;
        }
    }

    return true;
}

template <typename T>
bool RerankBase::convertSingleValue(const std::string& str, size_t* pHashValue) {
    T v;
    if (!autil::StringUtil::fromString<T>(str, v)) {
        SQL_LOG(ERROR, "Could not convert from [%s]", str.c_str());
        return false;
    }
    *pHashValue = autil::HashUtil::calculateHashValue<T>(v);

    return true;
}

template <typename T>
bool RerankBase::convertMultiValue(const std::string& str, size_t* pHashValue) {
    auto&& tmps = autil::StringUtil::split(str, MULTI_VALUE_SEP);
    if (tmps.size() == 0) {
        SQL_LOG(WARN, "Multi value is empty");
        *pHashValue = 0;
        return true;
    }
    using S = typename T::value_type;
    S v;
    std::vector<S> values;
    for (size_t i = 0; i < tmps.size(); i++) {
        if (!autil::StringUtil::fromString<S>(tmps[i], v)) {
            SQL_LOG(ERROR, "Could not convert from [%s]", tmps[i].c_str());
            return false;
        }
        values.push_back(v);
    }
    char* buffer = autil::MultiValueCreator::createMultiValueBuffer(values, _queryPool);
    autil::MultiValueType<S> mv(buffer);
    *pHashValue = autil::HashUtil::calculateHashValue<T>(mv);

    return true;
}

bool RerankBase::convertHashValue(const std::string& str,
                                  const matchdoc::ValueType& valueType,
                                  size_t* pHashValue) {
    using matchdoc::MatchDocBuiltinType2CppType;

    bool isMulti = valueType.isMultiValue();
    matchdoc::BuiltinType type = valueType.getBuiltinType();
    switch (type) {
#define CASE_CONVERT_MACRO(ft)                                             \
    case ft: {                                                             \
        if (isMulti) {                                                     \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;      \
                return convertMultiValue<T>(str, pHashValue);              \
            } else {                                                       \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                return convertSingleValue<T>(str, pHashValue);             \
            }                                                              \
            break;                                                         \
        }
    NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_CONVERT_MACRO);
    case matchdoc::BuiltinType::bt_string:
       if (isMulti) {
           auto&& tmps = autil::StringUtil::split(str, MULTI_VALUE_SEP);
           if (tmps.size() == 0) {
               SQL_LOG(WARN, "Multi value is empty");
               *pHashValue = 0;
               return true;
           }
           char* buffer = autil::MultiValueCreator::createMultiValueBuffer(tmps, _queryPool);
           autil::MultiString mv(buffer);
           *pHashValue = autil::HashUtil::calculateHashValue<autil::MultiString>(mv);
           return true;
       } else {
           char *buf = autil::MultiValueCreator::createMultiValueBuffer(
                   str.data(), str.size(), _queryPool);
           autil::MultiChar mc(buf);
           *pHashValue = autil::HashUtil::calculateHashValue<autil::MultiChar>(mc);
           return true;
       }
    case matchdoc::BuiltinType::bt_bool:
       if (isMulti) {
           AUTIL_LOG(ERROR, "multi bool type not supported");
           return false;
       } else {
           return false;
       }
#undef CASE_MACRO
    default:
        return false;
    }
}
bool RerankBase::makeInputHash(const table::TablePtr& table,
        std::vector<size_t>* pHashValues) {
    for (size_t i = 0; i < _quotaFieldValues.size(); i++) {
        for (size_t j = 0; j < _quotaFieldValues[i].size(); j++) {
            auto valueType = table->getColumn(_quotaFields[j])->getColumnSchema()->getType();
            size_t tmp = 0;
            if (!convertHashValue(_quotaFieldValues[i][j], valueType, &tmp)) {
                SQL_LOG(ERROR, "Failed to convert hash value from [%s]", _quotaFieldValues[i][j].c_str());
                return false;
            }
            if (j == 0) {
                (*pHashValues)[i] = tmp;
            } else {
                autil::HashUtil::combineHash((*pHashValues)[i], tmp);
            }
        }
    }

    return true;
}
bool RerankBase::fillResult(const table::TablePtr& table,
        const std::vector<size_t>& inputHashValues,
        std::vector<std::vector<table::Row> >* pGroups) {
    size_t rowCount = table->getRowCount();
    std::vector<size_t> hashValues;
    if (!table::TableUtil::calculateGroupKeyHash(table, _quotaFields, hashValues)) {
        SQL_LOG(ERROR, "Failed to calculate group key [%s] hash", _quotaFieldStr.c_str());
        return false;
    }

    auto findIndex = [&inputHashValues](const size_t val, size_t* pIndex) -> bool{
        for (size_t i = 0; i < inputHashValues.size(); i++) {
            if (inputHashValues[i] == val) {
                *pIndex = i;
                return true;
            }
        }

        return false;
    };

    for (size_t i = 0; i < rowCount; i++) {
        size_t idx = 0;
        if (!findIndex(hashValues[i], &idx)) {
            // actually, this could not be here
            // if here, exist one bug
            SQL_LOG(WARN, "Not match hash value");
        } else {
            (*pGroups)[idx].push_back(table->getRow(i));
        }
    }

    return true;
}
bool RerankBase::makeGroup(const table::TablePtr& table,
        std::vector<std::vector<table::Row> >* pGroups) {
    if (!checkQuotaField(table)) {
        SQL_LOG(ERROR, "Failed to check quota field");
        return false;
    }
    SQL_LOG(DEBUG, "Check quota field success");

    std::vector<size_t> hashValues;
    hashValues.resize(_quotaFieldValues.size());
    if (!makeInputHash(table, &hashValues)) {
        SQL_LOG(ERROR, "Failed to make input hash");
        return false;
    }
    fillResult(table, hashValues, pGroups);

    return true;
}
}; // namespace sql
} // namespace isearch
