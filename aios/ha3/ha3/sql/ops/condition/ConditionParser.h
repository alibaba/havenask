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

#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/condition/Condition.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace isearch {
namespace sql {

class ConditionParser {
public:
    ConditionParser(autil::mem_pool::Pool *pool = NULL);
    ~ConditionParser();

private:
    ConditionParser(const ConditionParser &);
    ConditionParser &operator=(const ConditionParser &);

public:
    bool parseCondition(const std::string &jsonStr, ConditionPtr &condition);

private:
    bool buildCondition(const autil::SimpleValue &jsonObject, ConditionPtr &condition);
    bool validateCondition(const ConditionPtr &condition) const {
        return true;
    }

private:
    autil::mem_pool::Pool *_pool;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ConditionParser> ConditionParserPtr;
} // namespace sql
} // namespace isearch
