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
#include "sql/ops/tvf/builtin/PrintTableTvfFunc.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/Row.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

const SqlTvfProfileInfo printTableTvfInfo("printTableTvf", "printTableTvf");
const string printTableTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "printTableTvf",
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

PrintTableTvfFunc::PrintTableTvfFunc() {
    _count = 0;
}

PrintTableTvfFunc::~PrintTableTvfFunc() {}

bool PrintTableTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 1) {
        SQL_LOG(WARN, "param size [%ld] not equal 1", context.params.size());
        return false;
    }
    if (!StringUtil::fromString(context.params[0], _count)) {
        SQL_LOG(WARN, "parse param [%s] fail.", context.params[0].c_str());
        return false;
    }
    return true;
}

bool PrintTableTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    SQL_LOG(INFO, "add debug info:\n %s", TableUtil::toString(input, _count).c_str());
    output = input;
    return true;
}

PrintTableTvfFuncCreator::PrintTableTvfFuncCreator()
    : TvfFuncCreatorR(printTableTvfDef, printTableTvfInfo) {}

PrintTableTvfFuncCreator::~PrintTableTvfFuncCreator() {}

TvfFunc *PrintTableTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new PrintTableTvfFunc();
}

REGISTER_RESOURCE(PrintTableTvfFuncCreator);

} // namespace sql
