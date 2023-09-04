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
#include "sql/ops/tableSplit/TableSplit.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <iterator>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/HashFuncFactory.h"
#include "autil/MultiValueType.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "sql/common/Log.h"
#include "sql/ops/util/KernelUtil.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace table;
using namespace autil;

namespace sql {

AUTIL_LOG_SETUP(sql, TableSplit);

namespace split_table {

struct CollectValues {
    HashFunctionBasePtr hashFunc;
    vector<string> values;
};

struct Collect1DValues : public CollectValues {
    Collect1DValues() {
        values.resize(1);
    }

    const vector<string> &collect(const std::vector<std::vector<std::string>> &multiValues,
                                  size_t offset) {
        values[0] = multiValues[0][offset];
        return values;
    }
};

struct Collect2DValues : public CollectValues {
    Collect2DValues() {
        values.resize(2);
    }

    const vector<string> &collect(const std::vector<std::vector<std::string>> &multiValues,
                                  size_t offset) {
        values[0] = multiValues[0][offset];
        values[1] = multiValues[1][offset];
        return values;
    }
};

struct CollectCommonValues : public CollectValues {
    CollectCommonValues(size_t n) {
        values.resize(n);
    }

    const vector<string> &collect(const std::vector<std::vector<std::string>> &multiValues,
                                  size_t offset) {
        for (size_t i = 0; i < values.size(); ++i) {
            values[i] = multiValues[i][offset];
        }
        return values;
    }
};

} // namespace split_table

void TableSplit::init(const TableDistribution &dist) {
    _partCnt = dist.partCnt;
    if (_partCnt <= 1) {
        _partCnt = 1;
        SQL_LOG(DEBUG, "single part distribution");
        return;
    }

    _hashMode = dist.hashMode;
    if (_hashMode._hashFields.empty()) {
        SQL_LOG(DEBUG, "empty hash field");
        return;
    }
    KernelUtil::stripName(_hashMode._hashField);
    KernelUtil::stripName(_hashMode._hashFields);

    SQL_LOG(DEBUG, "create hash fucntion: %s", _hashMode._hashFunction.c_str());
    _hashFunc = HashFuncFactory::createHashFunc(_hashMode._hashFunction, _hashMode._hashParams);
    if (_hashFunc == nullptr) {
        SQL_LOG(WARN, "create hash function %s failed", _hashMode._hashFunction.c_str());
        return;
    }

    auto rangeVec = RangeUtil::splitRange(0, RangeUtil::MAX_PARTITION_RANGE, dist.partCnt);
    const auto rangeSize = rangeVec.size();
    _rangeBounds.resize(rangeSize);
    for (size_t i = 0; i < rangeSize; ++i) {
        _rangeBounds[i] = rangeVec[i].second + 1;
    }
}

bool TableSplit::split(table::Table &table, std::vector<std::vector<table::Row>> &partRows) {
    partRows.clear();
    if (_partCnt <= 1 || _hashFunc == nullptr) {
        SQL_LOG(DEBUG, "no split need");
        return false;
    }
    std::vector<table::Column *> partCols;
    initPartColumns(table, partCols);
    if (partCols.empty()) {
        SQL_LOG(DEBUG, "empty part columns");
        return false;
    }
    return doSplit(table, partCols, partRows);
}

void TableSplit::initPartColumns(table::Table &table, std::vector<table::Column *> &partCols) {
    if (_hashFunc == nullptr || _hashMode._hashFields.size() == 0u) {
        SQL_LOG(DEBUG, "empty part columns");
        return;
    }
    const size_t partColSize = 1;
    partCols.reserve(partColSize);
    for (size_t i = 0; i < partColSize; ++i) {
        const auto &hashField = _hashMode._hashFields[i];
        auto col = getColumn(hashField, table);
        if (col == nullptr) {
            SQL_LOG(DEBUG, "invalid part field%ld:%s", i, hashField.c_str());
            return;
        }
        SQL_LOG(DEBUG, "valid part field%ld:%s", i, hashField.c_str());
        partCols.push_back(col);
    }
    return;
}

table::Column *TableSplit::getColumn(const std::string &field, table::Table &table) {
    auto col = table.getColumn(field);
    if (col == nullptr) {
        SQL_LOG(DEBUG, "hash field:%s not found", field.c_str());
        return nullptr;
    }
    if (col->getColumnSchema()->getType().isMultiValue()) {
        SQL_LOG(DEBUG, "hash field:%s is multi value, not support", field.c_str());
        return nullptr;
    }
    return col;
}

bool TableSplit::doSplit(table::Table &table,
                         const std::vector<table::Column *> &partCols,
                         std::vector<std::vector<table::Row>> &partRows) {
    const auto partColSize = partCols.size();
    vector<vector<string>> multiHashValues(partColSize);
    for (size_t i = 0; i < partColSize; ++i) {
        if (!collectHashValues(*(partCols[i]), multiHashValues[i])) {
            SQL_LOG(INFO, "collect hash values for column %ld failed", i);
            return false;
        }
    }
    partRows.resize(_partCnt);
    splitByHashValues(multiHashValues, table, partRows);
    return true;
}

bool TableSplit::collectHashValues(table::Column &col, std::vector<std::string> &hashValues) {
    auto singleFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto colData = col.getColumnData<T>();
        if (colData == nullptr) {
            return false;
        }
        const auto rowCount = col.getRowCount();
        hashValues.reserve(rowCount);
        for (size_t i = 0; i < rowCount; ++i) {
            hashValues.push_back(StringUtil::toString(colData->get(i)));
        }
        return true;
    };
    auto notSupportFunc = [&](auto a) -> bool { return false; };
    auto type = col.getColumnSchema()->getType();
    return ValueTypeSwitch::switchType(type, singleFunc, notSupportFunc);
}

void TableSplit::splitByHashValues(const std::vector<std::vector<std::string>> &multiHashValues,
                                   table::Table &table,
                                   std::vector<std::vector<table::Row>> &partRows) {
    splitByHashRange(multiHashValues, table, partRows);
}

void TableSplit::splitByHashRange(const std::vector<std::vector<std::string>> &multiHashValues,
                                  table::Table &table,
                                  std::vector<std::vector<table::Row>> &partRows) {
    auto n = multiHashValues.size();
    switch (n) {
    case 1:
        SQL_LOG(DEBUG, "Collect1DValues");
        splitByHashRange(multiHashValues, table, partRows, split_table::Collect1DValues());
        return;
    case 2:
        SQL_LOG(DEBUG, "Collect2DValues");
        splitByHashRange(multiHashValues, table, partRows, split_table::Collect2DValues());
        return;
    default:
        SQL_LOG(DEBUG, "CollectCommonValues(%ld)", n);
        splitByHashRange(multiHashValues, table, partRows, split_table::CollectCommonValues(n));
        return;
    }
}

template <class Func>
void TableSplit::splitByHashRange(const std::vector<std::vector<std::string>> &multiHashValues,
                                  table::Table &table,
                                  std::vector<std::vector<table::Row>> &partRows,
                                  Func func) {
    vector<bool> selectedPart;
    const auto rowCount = table.getRowCount();
    for (size_t i = 0; i < rowCount; ++i) {
        const auto &hashValues = func.collect(multiHashValues, i);
        auto ranges = _hashFunc->getHashRange(hashValues);
        auto row = table.getRow(i);
        selectedPart.assign(_partCnt, false);
        for (const auto &range : ranges) {
            auto partIds = range2partId(range);
            if (unlikely(partIds.first < 0 || partIds.second < 0 || partIds.first >= _partCnt
                         || partIds.second >= _partCnt)) {
                SQL_LOG(DEBUG, "invalid partId[%d:%d]", partIds.first, partIds.second);
                continue;
            }
            for (int i = partIds.first; i <= partIds.second; ++i) {
                if (unlikely(selectedPart[i])) {
                    continue;
                }
                selectedPart[i] = true;
                auto &rows = partRows[i];
                rows.push_back(row);
            }
        }
    }
}

int TableSplit::getPartId(uint32_t point) {
    auto iterBegin = _rangeBounds.begin();
    auto iterEnd = _rangeBounds.end();
    auto iter = upper_bound(iterBegin, iterEnd, point);
    if (unlikely(iter == iterEnd)) {
        return -1;
    }
    return distance(iterBegin, iter);
}

std::pair<int, int> TableSplit::range2partId(const autil::PartitionRange &range) {
    if (range.first == range.second) {
        auto partId = getPartId(range.first);
        return {partId, partId};
    } else {
        return {getPartId(range.first), getPartId(range.second)};
    }
}

} // namespace sql
