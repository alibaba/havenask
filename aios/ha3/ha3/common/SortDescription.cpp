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
#include "ha3/common/SortDescription.h"

#include <assert.h>
#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "autil/Log.h"

using namespace std;
using namespace suez::turing;

namespace autil {
DECLARE_SIMPLE_TYPE(isearch::common::SortDescription::RankSortType);
}

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SortDescription);

SortDescription::SortDescription() {
    _originalString = "";
    _sortAscendFlag = false;
    _type = RS_RANK;
    _rootSyntaxExpr = NULL;
}

SortDescription::SortDescription(const std::string &originalString)
{
    _originalString = originalString;
    _sortAscendFlag = false;
    _type = RS_RANK;
    _rootSyntaxExpr = NULL;
}

SortDescription::~SortDescription() {
    delete _rootSyntaxExpr;
    _rootSyntaxExpr = NULL;
}

const string& SortDescription::getOriginalString() const {
    return _originalString;
}

void SortDescription::setOriginalString(const std::string &originalString) {
    _originalString = originalString;
}

bool SortDescription::getSortAscendFlag() const {
    return _sortAscendFlag;
}

void SortDescription::setSortAscendFlag(bool sortAscendFlag) {
    _sortAscendFlag = sortAscendFlag;
}

SyntaxExpr* SortDescription::getRootSyntaxExpr() const {
    return _rootSyntaxExpr;
}

void SortDescription::setRootSyntaxExpr(SyntaxExpr* expr) {
    if (NULL != _rootSyntaxExpr) {
        DELETE_AND_SET_NULL(_rootSyntaxExpr);
    }
    _rootSyntaxExpr = expr;
}

void SortDescription::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_rootSyntaxExpr);
    dataBuffer.write(_originalString);
    dataBuffer.write(_sortAscendFlag);
    dataBuffer.write(_type);
}

void SortDescription::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_rootSyntaxExpr);
    dataBuffer.read(_originalString);
    dataBuffer.read(_sortAscendFlag);
    dataBuffer.read(_type);
}

string SortDescription::toString() const
{
    assert(_rootSyntaxExpr);
    if (_sortAscendFlag) {
        return "+(" + _rootSyntaxExpr->getExprString() + ")";
    } else {
        return "-(" + _rootSyntaxExpr->getExprString() + ")";
    }
}

} // namespace common
} // namespace isearch
