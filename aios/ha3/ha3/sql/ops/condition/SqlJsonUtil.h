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
#pragma once

#include <assert.h>
#include <string>

#include "autil/legacy/RapidJsonCommon.h"
#include "rapidjson/document.h"
#include "ha3/sql/common/common.h"

namespace isearch {
namespace sql {

class SqlJsonUtil {
public:
    static bool isColumn(const autil::SimpleValue &param) {
        return param.IsString() && param.GetStringLength() > 1
               && param.GetString()[0] == SQL_COLUMN_PREFIX;
    }
    static std::string getColumnName(const autil::SimpleValue &param) {
        assert(isColumn(param));
        return std::string(param.GetString()).substr(1);
    }
};

} // namespace sql
} // namespace isearch
