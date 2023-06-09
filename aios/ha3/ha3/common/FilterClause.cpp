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
#include "ha3/common/FilterClause.h"

#include <assert.h>
#include <stddef.h>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "autil/Log.h"

using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, FilterClause);

FilterClause::FilterClause() {
    _filterExpr = NULL;
}

FilterClause::FilterClause(SyntaxExpr *filterExpr) {
    _filterExpr = filterExpr;
}

FilterClause::~FilterClause() {
    DELETE_AND_SET_NULL(_filterExpr);
}

void FilterClause::setRootSyntaxExpr(SyntaxExpr *filterExpr) {
    if (_filterExpr != NULL) {
        delete _filterExpr;
        _filterExpr = NULL;
    }
    _filterExpr = filterExpr;
}

const SyntaxExpr *FilterClause::getRootSyntaxExpr() const {
    return _filterExpr;
}

void FilterClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_filterExpr);
}

void FilterClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_filterExpr);
}

std::string FilterClause::toString() const {
    assert(_filterExpr);
    return _filterExpr->getExprString();
}

} // namespace common
} // namespace isearch
