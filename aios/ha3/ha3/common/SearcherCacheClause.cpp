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
#include "ha3/common/SearcherCacheClause.h"

#include <stddef.h>

#include "autil/DataBuffer.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "ha3/common/FilterClause.h"
#include "autil/Log.h"

using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SearcherCacheClause);

SearcherCacheClause::SearcherCacheClause() {
    _use = false;
    _currentTime = INVALID_TIME;
    _key = 0L;
    _partRange.first = 0;
    _partRange.second = 0;
    _filterClause = NULL;
    _expireExpr = NULL;
    _cacheDocNumVec.push_back(200); // upward compatible with old cache doc num config
    _cacheDocNumVec.push_back(3000);
}

SearcherCacheClause::~SearcherCacheClause() {
    delete _filterClause;
    delete _expireExpr;
}

void SearcherCacheClause::setFilterClause(FilterClause *filterClause) {
    if (_filterClause != NULL) {
        delete _filterClause;
    }
    _filterClause = filterClause;
}

void SearcherCacheClause::setExpireExpr(SyntaxExpr *syntaxExpr) {
    if (_expireExpr != NULL) {
        delete _expireExpr;
    }
    _expireExpr = syntaxExpr;
}

void SearcherCacheClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_use);
    dataBuffer.write(_currentTime);
    dataBuffer.write(_key);
    dataBuffer.write(_cacheDocNumVec);
    dataBuffer.write(_filterClause);
    dataBuffer.write(_expireExpr);
    dataBuffer.write(_refreshAttributes);
}

void SearcherCacheClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_use);
    dataBuffer.read(_currentTime);
    dataBuffer.read(_key);
    dataBuffer.read(_cacheDocNumVec);
    dataBuffer.read(_filterClause);
    dataBuffer.read(_expireExpr);
    dataBuffer.read(_refreshAttributes);
}

std::string SearcherCacheClause::toString() const {
    return _originalString;
}

} // namespace common
} // namespace isearch
