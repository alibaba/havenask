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

#include <map>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/turbojetCalc/Common.h"
#include "turbojet/core/Graph.h"

namespace isearch::sql::turbojet {

class IquanPlanImporter {
public:
    static Result<LogicalGraphPtr> import(const CalcInitParam &outsJsonStr) {
        LogicalGraphPtr node;
        std::map<std::string, expr::SyntaxNodePtr> outMap;
        AR_RET_IF_ERR(import(outsJsonStr, node, outMap));
        return node;
    }

    static Result<> import(const CalcInitParam &outsJsonStr,
                           LogicalGraphPtr &node,
                           std::map<std::string, expr::SyntaxNodePtr> &outMap);

private:
    static Result<expr::SyntaxNodePtr>
    sqlOutput(const std::string &type, const std::string &name, const expr::SyntaxNodePtr &node);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace isearch::sql::turbojet
