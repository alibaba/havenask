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
#include "ha3/sql/ops/tvf/builtin/TopKTvfFunc.h"
#include "ha3/sql/ops/tvf/builtin/SortTvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"

using namespace std;
using namespace autil;
using namespace table;

namespace isearch {
namespace sql {
const SqlTvfProfileInfo topKTvfInfo("topKTvf", "topKTvf");
const string topKTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "topKTvf",
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

TopKTvfFuncCreator::TopKTvfFuncCreator()
    : TvfFuncCreatorR(topKTvfDef, topKTvfInfo) {}

TopKTvfFuncCreator::~TopKTvfFuncCreator() {}

TvfFunc *TopKTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new SortTvfFunc(false);
}

REGISTER_RESOURCE(TopKTvfFuncCreator);

} // namespace sql
} // namespace isearch
