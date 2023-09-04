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
#include "sql/ops/tvf/builtin/IdentityTvfFunc.h"

#include <iosfwd>

#include "navi/engine/Resource.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "table/Row.h"

using namespace std;
using namespace table;

namespace sql {

bool IdentityTvfFunc::init(TvfFuncInitContext &context) {
    return true;
}

bool IdentityTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    output = input;
    return true;
}

static const SqlTvfProfileInfo IDENTITY_TVF_FUNCINFO("identityTvf", "identityTvf");
static const string IDENTITY_TVF_DEF = R"tvf_json(
{
  "function_version": 1,
  "function_name": "identityTvf",
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

IdentityTvfFuncCreator::IdentityTvfFuncCreator()
    : TvfFuncCreatorR(IDENTITY_TVF_DEF, IDENTITY_TVF_FUNCINFO) {}

TvfFunc *IdentityTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new IdentityTvfFunc();
}

REGISTER_RESOURCE(IdentityTvfFuncCreator);

} // namespace sql
