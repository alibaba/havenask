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
#include "ha3/queryparser/VirtualAttributeParser.h"

#include <assert.h>
#include <iosfwd>

#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "autil/Log.h"

using namespace std;
using namespace isearch::common;
namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, VirtualAttributeParser);

VirtualAttributeParser::VirtualAttributeParser(ErrorResult *errorResult) 
    : _errorResult(errorResult)
{ 
}

VirtualAttributeParser::~VirtualAttributeParser() { 
}

common::VirtualAttributeClause* VirtualAttributeParser::createVirtualAttributeClause() {
    return new VirtualAttributeClause;
}

void VirtualAttributeParser::addVirtualAttribute(
        common::VirtualAttributeClause *virtualAttributeClause, 
        const std::string &name, 
        suez::turing::SyntaxExpr *syntaxExpr)
{
    assert(virtualAttributeClause);
    if (!virtualAttributeClause->addVirtualAttribute(name, syntaxExpr)) {
        delete syntaxExpr;
        _errorResult->resetError(ERROR_VIRTUALATTRIBUTE_CLAUSE,
                string("add virtualAttribute failed[") + name +
                string("], same name virtualAttribute may has already existed"));
    }
}

} // namespace queryparser
} // namespace isearch

