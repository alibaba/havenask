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
#include "sql/ops/tvf/builtin/DistinctTopNTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <unordered_map>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

const SqlTvfProfileInfo partialDistinctTopNTvfInfo("partialDistinctTopNTvf",
                                                   "partialDistinctTopNTvf");
const string partialDistinctTopNTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "partialDistinctTopNTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "normal_scope": true
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

PartialDistinctTopNTvfFunc::PartialDistinctTopNTvfFunc()
    : _totalLimit(0)
    , _perGroupLimit(0)
    , _queryPool(nullptr) {}

PartialDistinctTopNTvfFunc::~PartialDistinctTopNTvfFunc() {}

bool PartialDistinctTopNTvfFunc::init(TvfFuncInitContext &context) {
    static constexpr size_t PARAM_SIZE = 4;
    if (context.params.size() < PARAM_SIZE) {
        SQL_LOG(WARN, "param size [%ld] not equal %ld", context.params.size(), PARAM_SIZE);
        return false;
    }
    _groupStr = context.params[0];
    _groupFields = StringUtil::split(context.params[0], ",");
    _sortStr = context.params[1];
    _sortFields = StringUtil::split(context.params[1], ",");
    if (_sortFields.size() == 0) {
        SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
        return false;
    }
    for (size_t i = 0; i < _sortFields.size(); i++) {
        const string &field = _sortFields[i];
        if (field.size() == 0) {
            SQL_LOG(WARN, "parse order [%s] fail.", context.params[1].c_str());
            return false;
        }
        if (field[0] != '+' && field[0] != '-') {
            _sortFlags.push_back(false);
        } else if (field[0] == '+') {
            _sortFlags.push_back(false);
            _sortFields[i] = field.substr(1);
        } else {
            _sortFlags.push_back(true);
            _sortFields[i] = field.substr(1);
        }
    }
    if (!StringUtil::fromString(context.params[2], _totalLimit)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[2].c_str());
        return false;
    }
    if (_totalLimit < 0) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 0", context.params[2].c_str());
        return false;
    }
    if (!StringUtil::fromString(context.params[3], _perGroupLimit)) {
        _perGroupLimit = 0;
    }
    if (_perGroupLimit < 0) {
        SQL_LOG(WARN, "parse param [%s] fail. count < 0", context.params[3].c_str());
        return false;
    }
    _queryPool = context.queryPool;
    return true;
}

bool PartialDistinctTopNTvfFunc::computeTable(const table::TablePtr &table) {
    if (_totalLimit <= 0) {
        table->clearRows();
        return true;
    }
    ComboComparatorPtr comparator
        = ComparatorCreator::createComboComparator(table, _sortFields, _sortFlags, _queryPool);
    if (comparator == nullptr) {
        SQL_LOG(ERROR, "init distinctTopN tvf combo comparator [%s] failed.", _sortStr.c_str());
        return false;
    }
    vector<Row> sortRowVec = table->getRows();
    const auto &cmp = [&comparator](Row a, Row b) -> bool { return comparator->compare(a, b); };
    sort(sortRowVec.begin(), sortRowVec.end(), cmp);
    table->setRows(sortRowVec);
    sortRowVec.clear();
    if (table->getRowCount() <= (size_t)_totalLimit) {
        return true;
    }
    if (_groupFields.empty() || _perGroupLimit == 0) {
        table->clearBackRows(table->getRowCount() - (size_t)_totalLimit);
        return true;
    }
    vector<size_t> groupKeys;
    if (!TableUtil::calculateGroupKeyHash(table, _groupFields, groupKeys)) {
        SQL_LOG(ERROR, "calculate group key [%s] hash failed", _groupStr.c_str());
        return false;
    }
    if (table->getRowCount() != groupKeys.size()) {
        return false;
    }
    unordered_map<size_t, int64_t> groupCountMap;
    vector<Row> backupRowVec;
    groupCountMap.reserve(_totalLimit / _perGroupLimit);
    sortRowVec.reserve(_totalLimit);
    backupRowVec.reserve(_totalLimit);
    auto rawRowVec = table->getRows();
    for (size_t i = 0; i < rawRowVec.size(); ++i) {
        const auto &row = rawRowVec[i];
        auto groupKey = groupKeys[i];
        auto &groupCount = groupCountMap[groupKey];
        if (groupCount < _perGroupLimit) {
            sortRowVec.push_back(row);
            ++groupCount;
            if (sortRowVec.size() == (size_t)_totalLimit) {
                break;
            }
        } else if (backupRowVec.size() < (size_t)_totalLimit) {
            backupRowVec.push_back(row);
        }
    }
    if (sortRowVec.size() >= (size_t)_totalLimit) {
        table->setRows(sortRowVec);
        return true;
    }
    size_t backupCount = _totalLimit - sortRowVec.size();
    if (backupRowVec.size() > backupCount) {
        backupRowVec.erase(backupRowVec.begin() + backupCount, backupRowVec.end());
    }
    vector<Row> outRowVec(_totalLimit);
    merge(sortRowVec.begin(),
          sortRowVec.end(),
          backupRowVec.begin(),
          backupRowVec.end(),
          outRowVec.begin(),
          cmp);
    table->setRows(outRowVec);
    return true;
}

bool PartialDistinctTopNTvfFunc::doCompute(const table::TablePtr &input, table::TablePtr &output) {
    if (computeTable(input)) {
        output = input;
        return true;
    } else {
        return false;
    }
}

PartialDistinctTopNTvfFuncCreator::PartialDistinctTopNTvfFuncCreator()
    : TvfFuncCreatorR(partialDistinctTopNTvfDef, partialDistinctTopNTvfInfo) {}

PartialDistinctTopNTvfFuncCreator::~PartialDistinctTopNTvfFuncCreator() {}

TvfFunc *PartialDistinctTopNTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new PartialDistinctTopNTvfFunc();
}

REGISTER_RESOURCE(PartialDistinctTopNTvfFuncCreator);

const SqlTvfProfileInfo distinctTopNTvfInfo("distinctTopNTvf", "distinctTopNTvf");
const string distinctTopNTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "distinctTopNTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "normal_scope": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

DistinctTopNTvfFunc::DistinctTopNTvfFunc() {}

DistinctTopNTvfFunc::~DistinctTopNTvfFunc() {}

bool DistinctTopNTvfFunc::init(TvfFuncInitContext &context) {
    static constexpr size_t PARAM_SIZE = 6;
    if (context.params.size() < PARAM_SIZE) {
        SQL_LOG(WARN, "param size [%ld] not equal %ld", context.params.size(), PARAM_SIZE);
        return false;
    }
    if (!PartialDistinctTopNTvfFunc::init(context)) {
        return false;
    }
    if (getScope() != KernelScope::PARTIAL) {
        return true;
    }
    int64_t partTotalLimit = 0;
    StringUtil::fromString(context.params[4], partTotalLimit);
    if (partTotalLimit < 0) {
        partTotalLimit = 0;
    } else if (partTotalLimit > _totalLimit) {
        partTotalLimit = _totalLimit;
    }
    _totalLimit = partTotalLimit;

    int64_t partPerGroupLimit = 0;
    StringUtil::fromString(context.params[5], partPerGroupLimit);
    if (partPerGroupLimit < 0) {
        partPerGroupLimit = 0;
    } else if (partPerGroupLimit > _perGroupLimit) {
        partPerGroupLimit = _perGroupLimit;
    }
    _perGroupLimit = partPerGroupLimit;
    return true;
}

DistinctTopNTvfFuncCreator::DistinctTopNTvfFuncCreator()
    : TvfFuncCreatorR(distinctTopNTvfDef, distinctTopNTvfInfo) {}

DistinctTopNTvfFuncCreator::~DistinctTopNTvfFuncCreator() {}

TvfFunc *DistinctTopNTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new DistinctTopNTvfFunc();
}

REGISTER_RESOURCE(DistinctTopNTvfFuncCreator);

} // namespace sql
