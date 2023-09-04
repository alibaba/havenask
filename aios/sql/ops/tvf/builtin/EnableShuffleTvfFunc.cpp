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
#include "sql/ops/tvf/builtin/EnableShuffleTvfFunc.h"

#include <iosfwd>
#include <string>

#include "autil/TimeUtility.h"
#include "navi/engine/Resource.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "table/Row.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {
const SqlTvfProfileInfo enableShuffleTvfInfo("enableShuffleTvf", "enableShuffleTvf");
const string enableShuffleTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "enableShuffleTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": true
    },
    "prototypes": [
      {
        "params": {
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

EnableShuffleTvfFunc::EnableShuffleTvfFunc() {}

EnableShuffleTvfFunc::~EnableShuffleTvfFunc() {}

bool EnableShuffleTvfFunc::init(TvfFuncInitContext &context) {
    return true;
}

bool EnableShuffleTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    output = input;
    return true;
}

EnableShuffleTvfFuncCreator::EnableShuffleTvfFuncCreator()
    : TvfFuncCreatorR(enableShuffleTvfDef, enableShuffleTvfInfo) {}

EnableShuffleTvfFuncCreator::~EnableShuffleTvfFuncCreator() {}

TvfFunc *EnableShuffleTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new EnableShuffleTvfFunc();
}

REGISTER_RESOURCE(EnableShuffleTvfFuncCreator);

} // namespace sql
