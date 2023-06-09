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
#include "ha3/sql/ops/tvf/builtin/TopKMarkTvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "ha3/sql/common/Log.h"
#include "table/ComparatorCreator.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace isearch {
namespace sql {
const SqlTvfProfileInfo topKMarkTvfInfo("topKMarkTvf", "topKMarkTvf");
const string topKMarkTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "topKMarkTvf",
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
            {
              "field_name": "topKMark",
              "field_type": {
                "type": "int32"
              }
            }
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

TopKMarkTvfFuncCreator::TopKMarkTvfFuncCreator()
    : TvfFuncCreatorR(topKMarkTvfDef, topKMarkTvfInfo) {}

TopKMarkTvfFuncCreator::~TopKMarkTvfFuncCreator() {}

TvfFunc *TopKMarkTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new TopKMarkTvfFunc();
}

bool TopKMarkTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (input != nullptr) {
        if (!doCompute(input)) {
            return false;
        }
    }
    if (eof) {
        if (_comparator != nullptr && _table != nullptr) {
            TableUtil::topK(_table, _comparator.get(), _count, true);
            output = _table;
            Column *column = output->declareColumn<int32_t>(_markColumn);
            ColumnData<int32_t> *columnData = column->getColumnData<int32_t>();
            if (!columnData) {
                SQL_LOG(ERROR, "create mark column [%s] failed", _markColumn.c_str());
                return false;
            }
            for (size_t i = 0; i < output->getRowCount(); ++i) {
                if (i < _count) {
                    columnData->set(i, 1);
                } else {
                    columnData->set(i, 0);
                }
            }
        }
        SQL_LOG(DEBUG, "topK mark output table [%s] ", TableUtil::toString(output, 5).c_str());
    }
    return true;
}

bool TopKMarkTvfFunc::doCompute(const TablePtr &input) {
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
    return true;
}


REGISTER_RESOURCE(TopKMarkTvfFuncCreator);

} // namespace sql
} // namespace isearch
