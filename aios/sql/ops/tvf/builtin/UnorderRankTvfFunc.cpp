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
#include "sql/ops/tvf/builtin/UnorderRankTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/TableUtil.h"

namespace sql {
class TvfFunc;
} // namespace sql

using namespace std;
using namespace autil;
using namespace table;

namespace sql {
const SqlTvfProfileInfo unorderRankTvfInfo("unorderRankTvf", "unorderRankTvf");
const string unorderRankTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "unorderRankTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
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

UnorderRankTvfFunc::UnorderRankTvfFunc() {
    _count = 0;
}

UnorderRankTvfFunc::~UnorderRankTvfFunc() {}

bool UnorderRankTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
    ComboComparatorPtr comparator
        = ComparatorCreator::createComboComparator(input, _sortFields, _sortFlags, _queryPool);
    if (input->getRowCount() <= (size_t)_count) {
        output = input;
        return true;
    }
    if (comparator == nullptr) {
        SQL_LOG(ERROR, "init rank tvf combo comparator [%s] failed.", _sortStr.c_str());
        return false;
    }
    if (_partitionFields.empty()) {
        vector<Row> rowVec = input->getRows();
        vector<Row> sortRowVec = rowVec;
        TableUtil::topK(input, comparator.get(), _count, sortRowVec);
        input->setRows(sortRowVec);
    } else {
        vector<size_t> partitionKeys;
        if (!TableUtil::calculateGroupKeyHash(input, _partitionFields, partitionKeys)) {
            SQL_LOG(ERROR, "calculate partition key [%s] hash failed", _partitionStr.c_str());
            return false;
        }
        vector<Row> rowVec = input->getRows();
        if (partitionKeys.size() != rowVec.size()) {
            return false;
        }
        vector<pair<size_t, size_t>> keyOffsetVec;
        keyOffsetVec.resize(partitionKeys.size());
        for (size_t i = 0; i < partitionKeys.size(); i++) {
            keyOffsetVec[i] = make_pair(partitionKeys[i], i);
        }
        stable_sort(keyOffsetVec.begin(), keyOffsetVec.end());
        size_t lastKey = 0;
        vector<Row> sortRowVec;
        vector<Row> keepRowVec;
        for (size_t i = 0; i < keyOffsetVec.size(); i++) {
            if (lastKey == keyOffsetVec[i].first) {
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            } else {
                if (sortRowVec.size() > (size_t)_count) {
                    TableUtil::topK(input, comparator.get(), _count, sortRowVec);
                }
                keepRowVec.insert(keepRowVec.end(), sortRowVec.begin(), sortRowVec.end());
                lastKey = keyOffsetVec[i].first;
                sortRowVec.clear();
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            }
        }
        if (sortRowVec.size() > 0) {
            if (sortRowVec.size() > (size_t)_count) {
                TableUtil::topK(input, comparator.get(), _count, sortRowVec);
            }
            keepRowVec.insert(keepRowVec.end(), sortRowVec.begin(), sortRowVec.end());
        }
        input->setRows(keepRowVec);
    }
    output = input;
    return true;
}

UnorderRankTvfFuncCreator::UnorderRankTvfFuncCreator()
    : TvfFuncCreatorR(unorderRankTvfDef, unorderRankTvfInfo) {}

UnorderRankTvfFuncCreator::~UnorderRankTvfFuncCreator() {}

TvfFunc *UnorderRankTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new UnorderRankTvfFunc();
}

REGISTER_RESOURCE(UnorderRankTvfFuncCreator);

} // namespace sql
