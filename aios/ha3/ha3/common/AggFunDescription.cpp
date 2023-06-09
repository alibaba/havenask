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
#include "ha3/common/AggFunDescription.h"

#include <cstddef>

#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/RequestSymbolDefine.h"
#include "autil/Log.h"
#include "ha3/util/Serialize.h"

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AggFunDescription);

AggFunDescription::AggFunDescription() { 
    _syntaxExpr = NULL;
}

AggFunDescription::AggFunDescription(const string &functionName, 
                                     SyntaxExpr *syntaxExpr)
{
    _functionName = functionName;
    _syntaxExpr = syntaxExpr;
}

AggFunDescription::~AggFunDescription() { 
    delete _syntaxExpr;
}

void AggFunDescription::setFunctionName(const std::string& functionName) {
    _functionName = functionName;
}

const std::string& AggFunDescription::getFunctionName() const {
    return _functionName;
}

void AggFunDescription::setSyntaxExpr(SyntaxExpr *syntaxExpr) {
    if (_syntaxExpr) {
        delete _syntaxExpr;
    }
    _syntaxExpr = syntaxExpr;
}

SyntaxExpr* AggFunDescription::getSyntaxExpr() const {
    return _syntaxExpr;
}

#define AGGFUNDESCRIPTION_MEMBERS(X_X)          \
    X_X(_functionName);                         \
    X_X(_syntaxExpr);

HA3_SERIALIZE_IMPL(AggFunDescription, AGGFUNDESCRIPTION_MEMBERS);

std::string AggFunDescription::toString() const {
    return _functionName + AGGREGATE_DESCRIPTION_OPEN_PAREN
        + (_syntaxExpr ? _syntaxExpr->getExprString() : "")
        + AGGREGATE_DESCRIPTION_CLOSE_PAREN;
}

} // namespace common
} // namespace isearch

