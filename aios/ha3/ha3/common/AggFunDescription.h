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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/util/Serialize.h"

namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

class AggFunDescription
{
public:
    AggFunDescription();
    AggFunDescription(const std::string &functionName,
                      suez::turing::SyntaxExpr *syntaxExpr);
    ~AggFunDescription();
public:
    void setFunctionName(const std::string&);
    const std::string& getFunctionName() const;

    void setSyntaxExpr(suez::turing::SyntaxExpr*);
    suez::turing::SyntaxExpr* getSyntaxExpr() const;

    HA3_SERIALIZE_DECLARE();

    std::string toString() const;
private:
    std::string _functionName;
    suez::turing::SyntaxExpr* _syntaxExpr;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

