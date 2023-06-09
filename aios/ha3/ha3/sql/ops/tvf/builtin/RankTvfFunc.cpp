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
#include "ha3/sql/ops/tvf/builtin/RankTvfFunc.h"

#include <ext/alloc_traits.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "matchdoc/MatchDoc.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace isearch {
namespace sql {
const SqlTvfProfileInfo rankTvfInfo("rankTvf", "rankTvf");
const string rankTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "rankTvf",
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

RankTvfFunc::RankTvfFunc() {
    _count = 0;
}

RankTvfFunc::~RankTvfFunc() {}

struct RowHash {
    hash<uint32_t> _func;
    size_t operator()(const Row& row) const {
        return _func(row.getIndex());
    }
};

struct RowEqual {
    equal_to<uint32_t> _func;
    size_t operator()(const Row& a, const Row& b) const {
        return _func(a.getIndex(), b.getIndex());
    }
};

using RowSet = unordered_set<Row, RowHash, RowEqual>;

bool RankTvfFunc::doCompute(const TablePtr &input, TablePtr &output) {
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
        RowSet keepRowSet(sortRowVec.begin(), sortRowVec.end());
        vector<Row> keepRowVec; // to keep raw order
        for (auto row : rowVec) {
            if (keepRowSet.count(row) > 0) {
                keepRowVec.push_back(row);
            }
        }
        input->setRows(keepRowVec);
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
        RowSet keepRowSet;
        keepRowSet.reserve(rowVec.size());
        size_t lastKey = 0;
        vector<Row> sortRowVec;
        for (size_t i = 0; i < keyOffsetVec.size(); i++) {
            if (lastKey == keyOffsetVec[i].first) {
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            } else {
                if (sortRowVec.size() > (size_t)_count) {
                    TableUtil::topK(input, comparator.get(), _count, sortRowVec);
                }
                keepRowSet.insert(sortRowVec.begin(), sortRowVec.end());
                lastKey = keyOffsetVec[i].first;
                sortRowVec.clear();
                sortRowVec.push_back(rowVec[keyOffsetVec[i].second]);
            }
        }
        if (sortRowVec.size() > 0) {
            if (sortRowVec.size() > (size_t)_count) {
                TableUtil::topK(input, comparator.get(), _count, sortRowVec);
            }
            keepRowSet.insert(sortRowVec.begin(), sortRowVec.end());
        }
        vector<Row> keepRowVec; // to keep raw order
        for (auto row : rowVec) {
            if (keepRowSet.count(row) > 0) {
                keepRowVec.push_back(row);
            }
        }
        input->setRows(keepRowVec);
    }
    output = input;
    return true;
}

RankTvfFuncCreator::RankTvfFuncCreator()
    : TvfFuncCreatorR(rankTvfDef, rankTvfInfo) {}

RankTvfFuncCreator::~RankTvfFuncCreator() {}

TvfFunc *RankTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new RankTvfFunc();
}

REGISTER_RESOURCE(RankTvfFuncCreator);

} // namespace sql
} // namespace isearch
