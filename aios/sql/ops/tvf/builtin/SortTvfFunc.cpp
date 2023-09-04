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
#include "sql/ops/tvf/builtin/SortTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <limits>
#include <memory>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
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
const SqlTvfProfileInfo sortTvfInfo("sortTvf", "sortTvf");
const string sortTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "sortTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "return_const": {
        "modify_inputs": "null"
      }
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

SortTvfFunc::SortTvfFunc(bool ordered)
    : _ordered(ordered) {
    _count = 0;
}

SortTvfFunc::~SortTvfFunc() {}

bool SortTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 2) {
        SQL_LOG(WARN, "param size [%ld] not equal 2", context.params.size());
        return false;
    }
    _sortStr = context.params[0];
    _sortFields = StringUtil::split(_sortStr, ",");
    if (_sortFields.size() == 0) {
        SQL_LOG(WARN, "parse order [%s] fail.", _sortStr.c_str());
        return false;
    }
    for (size_t i = 0; i < _sortFields.size(); i++) {
        const string &field = _sortFields[i];
        if (field.size() == 0) {
            SQL_LOG(WARN, "parse order [%s] fail.", _sortStr.c_str());
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
    if (!StringUtil::fromString(context.params[1], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[1].c_str());
        return false;
    }
    if (_count < 0) {
        _count = std::numeric_limits<int32_t>::max();
    }
    _queryPool = context.queryPool;
    return true;
}

bool SortTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (input != nullptr) {
        if (!doCompute(input)) {
            return false;
        }
    }
    if (eof) {
        if (_ordered && _comparator != nullptr && _table != nullptr) {
            TableUtil::sort(_table, _comparator.get());
        }
        output = _table;
    }
    return true;
}

bool SortTvfFunc::doCompute(const TablePtr &input) {
    if (_comparator == nullptr) {
        _table = input;
        _comparator
            = ComparatorCreator::createComboComparator(input, _sortFields, _sortFlags, _queryPool);
        if (_comparator == nullptr) {
            SQL_LOG(ERROR, "init sort tvf combo comparator [%s] failed.", _sortStr.c_str());
            return false;
        }
    } else {
        if (!_table->merge(input)) {
            SQL_LOG(ERROR, "sort tvf merge input table failed");
            return false;
        }
    }
    TableUtil::topK(_table, _comparator.get(), _count);
    _table->compact();
    return true;
}

SortTvfFuncCreator::SortTvfFuncCreator()
    : TvfFuncCreatorR(sortTvfDef, sortTvfInfo) {}

SortTvfFuncCreator::~SortTvfFuncCreator() {}

TvfFunc *SortTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new SortTvfFunc();
}

REGISTER_RESOURCE(SortTvfFuncCreator);

} // namespace sql
