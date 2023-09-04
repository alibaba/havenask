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
#include "ha3/search/MultiQueryExecutor.h"

#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/search/QueryExecutor.h"

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, MultiQueryExecutor);

MultiQueryExecutor::MultiQueryExecutor() {}

MultiQueryExecutor::~MultiQueryExecutor() {
    for (QueryExecutorVector::iterator it = _queryExecutors.begin(); it != _queryExecutors.end();
         it++) {
        POOL_DELETE_CLASS(*it);
    }
    _queryExecutors.clear();
}

const std::string MultiQueryExecutor::getName() const {
    return "MultiQueryExecutor";
}

// docid_t MultiQueryExecutor::getDocId() {
//     assert(false);
//     return INVALID_DOCID;
// }

/*
docid_t MultiQueryExecutor::seek(docid_t id) {
    assert(false);
    return INVALID_DOCID;
}
*/

void MultiQueryExecutor::moveToEnd() {
    QueryExecutor::moveToEnd();
    for (QueryExecutorVector::const_iterator it = _queryExecutors.begin();
         it != _queryExecutors.end();
         it++) {
        (*it)->moveToEnd();
    }
}

void MultiQueryExecutor::setEmpty() {
    QueryExecutor::setEmpty();
    for (QueryExecutorVector::const_iterator it = _queryExecutors.begin();
         it != _queryExecutors.end();
         it++) {
        (*it)->setEmpty();
    }
}

void MultiQueryExecutor::setCurrSub(docid_t docid) {
    QueryExecutor::setCurrSub(docid);
    for (auto it = _queryExecutors.begin(); it != _queryExecutors.end(); it++) {
        (*it)->setCurrSub(docid);
    }
}

uint32_t MultiQueryExecutor::getSeekDocCount() {
    uint32_t ret = 0;
    for (auto it = _queryExecutors.begin(); it != _queryExecutors.end(); it++) {
        ret += (*it)->getSeekDocCount();
    }
    return ret;
}

void MultiQueryExecutor::reset() {
    QueryExecutor::reset();
    for (QueryExecutorVector::const_iterator it = _queryExecutors.begin();
         it != _queryExecutors.end();
         it++) {
        (*it)->reset();
    }
}

std::string MultiQueryExecutor::toString() const {
    return convertToString(_queryExecutors);
}

} // namespace search
} // namespace isearch
