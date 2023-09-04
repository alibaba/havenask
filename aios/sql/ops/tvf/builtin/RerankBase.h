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
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/tvf/TvfFunc.h"
#include "table/ComboComparator.h"
#include "table/Row.h"
#include "table/Table.h"

namespace matchdoc {
struct ValueType;
} // namespace matchdoc

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace sql {
const std::string INPUT_VALUE_SEP = ";";
const std::string INPUT_VALUE_INNER_SEP = ",";
const std::string MULTI_VALUE_SEP = "#";

class mycomparison {
    table::ComboComparatorPtr comparator;
    bool reverse;

public:
    explicit mycomparison(const table::ComboComparatorPtr &cmp, const bool revParam = false)
        : comparator(cmp)
        , reverse(revParam) {}
    bool operator()(const table::Row &lhs, const table::Row &rhs) const {
        if (reverse) {
            return !comparator->compare(lhs, rhs);
        } else {
            return comparator->compare(lhs, rhs);
        }
    }
};

class RerankBase : public OnePassTvfFunc {
public:
    RerankBase() = default;
    ~RerankBase() = default;
    RerankBase(const RerankBase &) = delete;
    RerankBase &operator=(const RerankBase &) = delete;

public:
    bool init(TvfFuncInitContext &context) override;
    bool doCompute(const table::TablePtr &input, table::TablePtr &output) override;

protected:
    virtual bool computeTable(const table::TablePtr &input);

    bool checkQuotaField(const table::TablePtr &table);
    template <typename T>
    bool convertSingleValue(const std::string &str, size_t *pHashValue);
    template <typename T>
    bool convertMultiValue(const std::string &str, size_t *pHashValue);
    bool convertHashValue(const std::string &str,
                          const matchdoc::ValueType &valueType,
                          size_t *pHashValue);
    bool makeInputHash(const table::TablePtr &table, std::vector<size_t> *pHashValues);

    bool fillResult(const table::TablePtr &table,
                    const std::vector<size_t> &inputHashValues,
                    std::vector<std::vector<table::Row>> *pGroups);

    bool makeGroup(const table::TablePtr &table, std::vector<std::vector<table::Row>> *pGroups);

protected:
    std::vector<std::string> _quotaFields;
    std::vector<std::vector<std::string>> _quotaFieldValues;
    std::string _quotaFieldStr;      // a;b
    std::string _quotaFieldValueStr; // 1,2;3,4

    autil::mem_pool::Pool *_queryPool;
};

typedef std::shared_ptr<RerankBase> RerankBasePtr;
} // namespace sql
